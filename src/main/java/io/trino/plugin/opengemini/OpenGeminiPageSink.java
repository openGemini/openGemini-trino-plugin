/* Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.opengemini;

import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;

import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static io.trino.plugin.opengemini.OpenGeminiColumn.FIELD_KIND;
import static io.trino.plugin.opengemini.OpenGeminiColumn.TAG_KIND;
import static io.trino.plugin.opengemini.OpenGeminiColumn.TIME_KIND;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_NANOS;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static io.trino.spi.type.Timestamps.roundDiv;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class OpenGeminiPageSink
        implements ConnectorPageSink
{
    private static final Logger log = Logger.get(OpenGeminiPageSink.class);

    private final OpenGeminiSession openGeminiSession;
    private final String schemaName;
    private final String rpName;
    private final String tableName;
    private final List<String> columnNames;
    private final List<Type> columnTypes;
    private final List<String> columnKinds;

    public OpenGeminiPageSink(OpenGeminiSession session, OpenGeminiInsertTableHandle handler)
    {
        this.openGeminiSession = session;
        this.schemaName = handler.getSchemaName();
        this.rpName = handler.getRpName();
        this.tableName = handler.getTableName();
        this.columnNames = handler.getColumnNames();
        this.columnTypes = handler.getColumnTypes();
        this.columnKinds = handler.getColumnKinds();
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        log.info("debug for appendPage, db: %s, rp: %s, measurement: %s", schemaName, rpName, tableName);
        BatchPoints batchPoints = BatchPoints.database(schemaName).retentionPolicy(rpName).build();
        for (int position = 0; position < page.getPositionCount(); position++) {
            batchPoints.point(appendOnePage(page, position));
        }
        openGeminiSession.writePoints(batchPoints);
        return NOT_BLOCKED;
    }

    private Point appendOnePage(Page page, int position)
    {
        long timestamp = -1;
        Map<String, Object> fieldsToAdd = new HashMap<>();
        Map<String, String> tagsToAdd = new HashMap<>();
        for (int channel = 0; channel < page.getChannelCount(); channel++) {
            String name = columnNames.get(channel);
            Type type = columnTypes.get(channel);
            String kind = columnKinds.get(channel);

            Block block = page.getBlock(channel);
            if (block.isNull(position)) {
                log.info("debug for appendOnePage, column: %s, kind: %s, value is null", name, kind);
                continue;
            }

            Object val = getValue(block, position, type);
            if (val == null) {
                continue;
            }

            switch (kind) {
                case TIME_KIND -> { timestamp = (long) val; }
                case TAG_KIND -> tagsToAdd.put(name, (String) val);
                case FIELD_KIND -> fieldsToAdd.put(name, val);
                default -> log.warn("unsupported kind: %s", kind);
            }
            log.info("debug for appendOnePage, column: %s, kind: %s, type: %s, val: %s", name, kind, type, val);
        }

        if (timestamp < 0) {
            timestamp = currentTimeNanos();
        }
        return Point.measurement(tableName).tag(tagsToAdd).fields(fieldsToAdd).time(timestamp, TimeUnit.NANOSECONDS).build();
    }

    private Object getValue(Block block, int position, Type type)
    {
        if (TIMESTAMP_TZ_NANOS.equals(type)) {
            LongTimestampWithTimeZone value = (LongTimestampWithTimeZone) type.getObject(block, position);
            return value.getEpochMillis() * NANOSECONDS_PER_MILLISECOND + roundDiv(value.getPicosOfMilli(), PICOSECONDS_PER_NANOSECOND);
        }
        else if (type instanceof VarcharType) {
            return type.getSlice(block, position).toStringUtf8();
        }
        else if (BIGINT.equals(type)) {
            return type.getLong(block, position);
        }
        else if (DOUBLE.equals(type)) {
            return type.getDouble(block, position);
        }
        else if (BOOLEAN.equals(type)) {
            return type.getBoolean(block, position);
        }
        else {
            log.warn("unsupported type %s", type);
        }
        return null;
    }

    private long currentTimeNanos()
    {
        Instant now = Instant.now();
        return now.toEpochMilli() * NANOSECONDS_PER_MILLISECOND + now.getNano();
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        // the committer does not need any additional info
        return completedFuture(ImmutableList.of());
    }

    @Override
    public void abort() {}
}

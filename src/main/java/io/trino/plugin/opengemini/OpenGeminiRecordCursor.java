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

import com.google.common.base.Strings;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.Type;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_NANOS;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;

public class OpenGeminiRecordCursor
        implements RecordCursor
{
    private final List<OpenGeminiColumnHandle> columnHandles;
    private final boolean hasTimeColumn;
    private final OpenGeminiQueryResultIterator iter;

    private List<Object> row = new ArrayList<>();

    public OpenGeminiRecordCursor(OpenGeminiSession openGeminiSession, List<OpenGeminiColumnHandle> columnHandles, String database, String sql)
    {
        this.columnHandles = columnHandles;
        // column time would be the first if existed; skip its result if it is not in the querying columns.
        this.hasTimeColumn = columnHandles.isEmpty() || columnHandles.get(0).getColumnKind().equals(OpenGeminiColumn.TIME_KIND);

        this.iter = openGeminiSession.queryResultByChunk(database, sql);
    }

    @Override
    public long getCompletedBytes()
    {
        return 0;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public Type getType(int field)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return columnHandles.get(field).getColumnType();
    }

    @Override
    public boolean advanceNextPosition()
    {
        if (iter.hasNext()) {
            row = iter.getRow();
            return true;
        }
        return false;
    }

    private String getFieldValue(int field)
    {
        if (!hasTimeColumn) {
            field += 1;
        }
        checkState(row != null, "Cursor has not been advanced yet");
        Object value = row.get(field);
        return value == null ? "" : value.toString();
    }

    @Override
    public boolean getBoolean(int field)
    {
        checkFieldType(field, BOOLEAN);
        return Boolean.parseBoolean(getFieldValue(field));
    }

    @Override
    public long getLong(int field)
    {
        checkFieldType(field, BIGINT);
        return (long) Double.parseDouble(getFieldValue(field));
    }

    @Override
    public double getDouble(int field)
    {
        checkFieldType(field, DOUBLE);
        return Double.parseDouble(getFieldValue(field));
    }

    @Override
    public Slice getSlice(int field)
    {
        checkFieldType(field, createUnboundedVarcharType());
        return Slices.utf8Slice(getFieldValue(field));
    }

    @Override
    public Object getObject(int field)
    {
        if (TIMESTAMP_TZ_NANOS.equals(getType(field))) {
            String fieldValue = getFieldValue(field);
            Instant instant = ZonedDateTime.parse(fieldValue, DateTimeFormatter.ISO_ZONED_DATE_TIME).toInstant();
            long fractionInPicos = (long) instant.getNano() * PICOSECONDS_PER_NANOSECOND;
            return LongTimestampWithTimeZone.fromEpochSecondsAndFraction(instant.getEpochSecond(), fractionInPicos, UTC_KEY);
        }
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isNull(int field)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return Strings.isNullOrEmpty(getFieldValue(field));
    }

    private void checkFieldType(int field, Type expected)
    {
        Type actual = getType(field);
        checkArgument(actual.equals(expected), "Expected field %s to be type %s but is %s", field, expected, actual);
    }

    @Override
    public void close()
    {
    }
}

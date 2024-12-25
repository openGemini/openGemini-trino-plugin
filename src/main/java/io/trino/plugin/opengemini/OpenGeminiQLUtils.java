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

import io.airlift.slice.Slice;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_NANOS;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;

public final class OpenGeminiQLUtils
{
    private OpenGeminiQLUtils() {}

    public static String selectFrom(OpenGeminiTableHandle tableHandle, List<OpenGeminiColumnHandle> columns, TupleDomain<ColumnHandle> tupleDomain)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("select ");
        if (columns.isEmpty()) {
            sb.append("* ");
        }
        else {
            sb.append(columns.stream().map(column -> toDoubleQuoted(column.getColumnName())).collect(Collectors.joining(","))).append(" ");
        }
        sb.append("from ").append(toDoubleQuoted(tableHandle.getRpName())).append(".").append(toDoubleQuoted(tableHandle.getTableName()));
        whereClause(tupleDomain, sb);
        orderByClause(tableHandle.getAscending(), sb);
        limitClause(tableHandle.getLimit(), sb);
        return sb.toString();
    }

    private static void orderByClause(boolean ascending, StringBuilder sb)
    {
        if (!ascending) {
            sb.append(" order by time desc");
        }
    }

    private static void limitClause(long limit, StringBuilder sb)
    {
        // in openGemini, limit num type is int
        if (limit > 0 && limit < Integer.MAX_VALUE) {
            sb.append(" limit ").append(limit);
        }
    }

    private static void whereClause(TupleDomain<ColumnHandle> tupleDomain, StringBuilder sb)
    {
        if (tupleDomain.isAll() || tupleDomain.getDomains().isEmpty()) {
            return;
        }

        boolean newEntry = false;
        StringBuilder whereBuiler = new StringBuilder();
        for (Map.Entry<ColumnHandle, Domain> entry : tupleDomain.getDomains().get().entrySet()) {
            OpenGeminiColumnHandle column = (OpenGeminiColumnHandle) entry.getKey();
            String fieldName = column.getColumnName();
            Type type = column.getColumnType();

            int newRange = 0;
            StringBuilder entryBuiler = new StringBuilder();
            for (Range range : entry.getValue().getValues().getRanges().getOrderedRanges()) {
                if (newRange > 0) {
                    entryBuiler.append(" or ");
                }
                entryBuiler.append(shouldClauses(range, fieldName, type));
                newRange++;
            }
            if (newRange > 1) {
                entryBuiler.insert(0, "(").append(")");
            }
            if (newEntry) {
                whereBuiler.append(" and ");
            }
            whereBuiler.append(entryBuiler);
            newEntry = true;
        }
        if (!whereBuiler.isEmpty()) {
            sb.append(" where ").append(whereBuiler);
        }
    }

    private static StringBuilder shouldClauses(Range range, String fieldName, Type type)
    {
        StringBuilder rangeBuiler = new StringBuilder();
        if (range.isSingleValue()) {
            rangeBuiler.append(toDoubleQuoted(fieldName)).append(" = ").append(convertValue(type, range.getSingleValue()));
        }
        else {
            boolean append = false;
            if (!range.isLowUnbounded()) {
                String op = range.isLowInclusive() ? ">=" : ">";
                rangeBuiler.append(toDoubleQuoted(fieldName)).append(" ").append(op).append(" ").append(convertValue(type, range.getLowBoundedValue()));
                append = true;
            }
            if (!range.isHighUnbounded()) {
                if (append) {
                    rangeBuiler.append(" and ");
                }
                String op = range.isHighInclusive() ? "<=" : "<";
                rangeBuiler.append(toDoubleQuoted(fieldName)).append(" ").append(op).append(" ").append(convertValue(type, range.getHighBoundedValue()));
            }
        }
        return rangeBuiler;
    }

    private static Object convertValue(Type type, Object value)
    {
        if (value == null) {
            return null;
        }

        if (type instanceof VarcharType) {
            return "'" + ((Slice) value).toStringUtf8() + "'";
        }

        if (DOUBLE.equals(type)) {
            return ((Number) value).doubleValue();
        }

        if (BIGINT.equals(type)) {
            return ((Number) value).longValue();
        }

        if (BOOLEAN.equals(type)) {
            return value;
        }

        if (TIMESTAMP_TZ_NANOS.equals(type)) {
            LongTimestampWithTimeZone v = (LongTimestampWithTimeZone) value;
            return v.getEpochMillis() * NANOSECONDS_PER_MILLISECOND + toIntExact(v.getPicosOfMilli() / PICOSECONDS_PER_NANOSECOND);
        }

        throw new IllegalArgumentException("unhandled type: " + type);
    }

    public static String toDoubleQuoted(String tableName)
    {
        return format("\"%s\"", tableName);
    }
}

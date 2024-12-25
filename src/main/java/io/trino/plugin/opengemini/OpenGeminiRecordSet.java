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
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.RecordSet;
import io.trino.spi.type.Type;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class OpenGeminiRecordSet
        implements RecordSet
{
    private final OpenGeminiSession openGeminiSession;
    private final List<OpenGeminiColumnHandle> columnHandles;
    private final List<Type> columnTypes;

    private final String database;
    private final String sql;

    public OpenGeminiRecordSet(OpenGeminiSession openGeminiSession, List<OpenGeminiColumnHandle> columnHandles, String database, String sql)
    {
        this.openGeminiSession = requireNonNull(openGeminiSession, "sesion is null");
        this.database = requireNonNull(database, "database is null");
        this.sql = requireNonNull(sql, "sql is null");
        this.columnHandles = requireNonNull(columnHandles, "columnHandles is null");

        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (OpenGeminiColumnHandle column : columnHandles) {
            types.add(column.getColumnType());
        }
        this.columnTypes = types.build();
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor()
    {
        return new OpenGeminiRecordCursor(openGeminiSession, columnHandles, database, sql);
    }
}

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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.type.Type;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class OpenGeminiInsertTableHandle
        implements ConnectorInsertTableHandle
{
    private final String schemaName;
    private final String rpName;
    private final String tableName;
    private final List<String> columnNames;
    private final List<Type> columnTypes;
    private final List<String> columnKinds;

    @JsonCreator
    public OpenGeminiInsertTableHandle(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("rpName") String rpName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("columnNames") List<String> columnNames,
            @JsonProperty("columnTypes") List<Type> columnTypes,
            @JsonProperty("columnKinds") List<String> columnKinds)
    {
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.rpName = requireNonNull(rpName, "rpName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.columnNames = requireNonNull(columnNames, "columnNames is null");
        this.columnTypes = requireNonNull(columnTypes, "columnTypes is null");
        this.columnKinds = requireNonNull(columnKinds, "columnKinds is null");
    }

    @JsonProperty
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public String getRpName()
    {
        return rpName;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public List<String> getColumnNames()
    {
        return columnNames;
    }

    @JsonProperty
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    @JsonProperty
    public List<String> getColumnKinds()
    {
        return columnKinds;
    }

    @Override
    public String toString()
    {
        return "opengemini:" + schemaName + "." + rpName + "." + tableName;
    }
}

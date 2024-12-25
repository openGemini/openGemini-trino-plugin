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
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public final class OpenGeminiTableHandle
        implements ConnectorTableHandle
{
    private final String schemaName;
    private final String rpName;
    private final String tableName;
    private final TupleDomain<ColumnHandle> constraint;
    private final long limit;
    private final boolean ascending;

    public OpenGeminiTableHandle(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("rpName") String rpName,
            @JsonProperty("tableName") String tableName)
    {
        this(schemaName, rpName, tableName, TupleDomain.all(), 0, true);
    }

    @JsonCreator
    public OpenGeminiTableHandle(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("rpName") String rpName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("constraint") TupleDomain<ColumnHandle> constraint,
            @JsonProperty("limit") long limit,
            @JsonProperty("ascending") boolean ascending)
    {
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.rpName = requireNonNull(rpName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.constraint = requireNonNull(constraint, "constraint is null");
        this.limit = limit;
        this.ascending = ascending;
    }

    public OpenGeminiTableHandle withConstraint(TupleDomain<ColumnHandle> constraint)
    {
        return new OpenGeminiTableHandle(this.schemaName, this.rpName, this.tableName, constraint, this.limit, this.ascending);
    }

    public OpenGeminiTableHandle withLimit(long limit)
    {
        return new OpenGeminiTableHandle(this.schemaName, this.rpName, this.tableName, this.constraint, limit, this.ascending);
    }

    public OpenGeminiTableHandle withAscending(boolean ascending)
    {
        return new OpenGeminiTableHandle(this.schemaName, this.rpName, this.tableName, this.constraint, this.limit, ascending);
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
    public TupleDomain<ColumnHandle> getConstraint()
    {
        return constraint;
    }

    @JsonProperty
    public long getLimit()
    {
        return limit;
    }

    @JsonProperty
    public boolean getAscending()
    {
        return ascending;
    }

    public SchemaTableName toSchemaTableName()
    {
        return new SchemaTableName(schemaName, tableName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaName, rpName, tableName);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }

        OpenGeminiTableHandle other = (OpenGeminiTableHandle) obj;
        return Objects.equals(this.schemaName, other.schemaName) &&
                Objects.equals(this.rpName, other.rpName) &&
                Objects.equals(this.tableName, other.tableName);
    }

    @Override
    public String toString()
    {
        return schemaName + ":" + rpName + ":" + tableName;
    }
}

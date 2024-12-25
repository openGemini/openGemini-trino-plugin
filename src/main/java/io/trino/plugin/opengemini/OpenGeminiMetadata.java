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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorOutputMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.LimitApplicationResult;
import io.trino.spi.connector.RetryMode;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.SortItem;
import io.trino.spi.connector.TableColumnsMetadata;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.connector.TopNApplicationResult;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.statistics.ComputedStatistics;
import io.trino.spi.type.Type;

import javax.inject.Inject;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Strings.isNullOrEmpty;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.connector.RetryMode.NO_RETRIES;
import static java.util.Objects.requireNonNull;

public class OpenGeminiMetadata
        implements ConnectorMetadata
{
    private static final Logger log = Logger.get(OpenGeminiMetadata.class);

    private final OpenGeminiSession openGeminiSession;

    @Inject
    public OpenGeminiMetadata(OpenGeminiSession openGeminiSession)
    {
        this.openGeminiSession = requireNonNull(openGeminiSession, "exampleClient is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return listSchemaNames();
    }

    public List<String> listSchemaNames()
    {
        return ImmutableList.copyOf(openGeminiSession.getSchemaNames());
    }

    public List<String> listTableNames(String schema)
    {
        return ImmutableList.copyOf(openGeminiSession.getTableNames(schema));
    }

    @Override
    public OpenGeminiTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        String schemaName = tableName.getSchemaName();
        if (!listSchemaNames(session).contains(schemaName)) {
            log.warn("schema %s not existed when get table handle", schemaName);
            return null;
        }

        String rpName = null;
        String measurementName = tableName.getTableName();
        // if table name like "<rp>.<table>", no space
        if (measurementName.contains(".")) {
            String[] parts = measurementName.split("\\.");
            if (parts.length != 2 || isNullOrEmpty(parts[0]) || isNullOrEmpty(parts[1])) {
                log.warn("invaild table name %s for schema %s when get table handler", measurementName, schemaName);
                return null;
            }
            rpName = parts[0];
            measurementName = parts[1];
            log.info("use specified rp %s for schema %s measurement %s", rpName, schemaName, measurementName);
        }
        if (!listTableNames(schemaName).contains(measurementName)) {
            log.warn("table %s not existed for schema %s when get table handler", measurementName, schemaName);
            return null;
        }

        if (isNullOrEmpty(rpName)) {
            rpName = openGeminiSession.getDefaultRpName(schemaName);
            log.info("use default rp %s for schema %s measurement %s", rpName, schemaName, measurementName);
        }

        return new OpenGeminiTableHandle(schemaName, rpName, measurementName);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> optionalSchemaName)
    {
        Set<String> schemaNames = optionalSchemaName.map(ImmutableSet::of)
                .orElseGet(() -> ImmutableSet.copyOf(openGeminiSession.getSchemaNames()));

        ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();
        for (String schemaName : schemaNames) {
            for (String tableName : openGeminiSession.getTableNames(schemaName)) {
                builder.add(new SchemaTableName(schemaName, tableName));
            }
        }
        return builder.build();
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        OpenGeminiTableHandle openGeminiTableHandle = (OpenGeminiTableHandle) tableHandle;
        OpenGeminiTable table = openGeminiSession.getTable(openGeminiTableHandle.getSchemaName(), openGeminiTableHandle.getTableName());
        if (table == null) {
            throw new TableNotFoundException(openGeminiTableHandle.toSchemaTableName());
        }

        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        for (OpenGeminiColumn column : table.getColumns()) {
            columnHandles.put(column.getName(), new OpenGeminiColumnHandle(column.getName(), column.getType(), column.getKind()));
        }
        return columnHandles.buildOrThrow();
    }

    @Override
    public Iterator<TableColumnsMetadata> streamTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName tableName : listTables(session, prefix)) {
            ConnectorTableMetadata tableMetadata = getTableMetadata(tableName);
            // table can disappear during listing operation
            if (tableMetadata != null) {
                columns.put(tableName, tableMetadata.getColumns());
            }
        }
        return columns.buildOrThrow().entrySet().stream()
                .map(e -> TableColumnsMetadata.forTable(e.getKey(), e.getValue()))
                .iterator();
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        return getTableMetadata(((OpenGeminiTableHandle) table).toSchemaTableName());
    }

    private ConnectorTableMetadata getTableMetadata(SchemaTableName tableName)
    {
        String schemaName = tableName.getSchemaName();
        if (!listSchemaNames().contains(schemaName)) {
            log.warn("schema %s not existed when get table metadata", schemaName);
            return null;
        }

        String measurementName = tableName.getTableName();
        OpenGeminiTable table = openGeminiSession.getTable(schemaName, measurementName);
        if (table == null) {
            log.warn("table is null for schema %s measurement %s when get table metadata", schemaName, measurementName);
            return null;
        }

        return new ConnectorTableMetadata(tableName, table.getColumnsMetadata());
    }

    private List<SchemaTableName> listTables(ConnectorSession session, SchemaTablePrefix prefix)
    {
        if (prefix.getTable().isEmpty()) {
            return listTables(session, prefix.getSchema());
        }
        return ImmutableList.of(prefix.toSchemaTableName());
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return ((OpenGeminiColumnHandle) columnHandle).getColumnMetadata();
    }

    @Override
    public Optional<LimitApplicationResult<ConnectorTableHandle>> applyLimit(ConnectorSession session, ConnectorTableHandle handle, long limit)
    {
        OpenGeminiTableHandle tableHandle = (OpenGeminiTableHandle) handle;
        long oldLimit = tableHandle.getLimit();
        if (oldLimit == limit) {
            return Optional.empty();
        }
        OpenGeminiTableHandle newHandle = tableHandle.withLimit(limit);
        return Optional.of(new LimitApplicationResult<>(newHandle, true, false));
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session, ConnectorTableHandle tableHandle, Constraint constraint)
    {
        OpenGeminiTableHandle handle = (OpenGeminiTableHandle) tableHandle;
        TupleDomain<ColumnHandle> oldDomain = handle.getConstraint();
        TupleDomain<ColumnHandle> newDomain = oldDomain.intersect(constraint.getSummary());
        if (oldDomain.equals(newDomain)) {
            // Nothing has changed, return empty Option
            return Optional.empty();
        }

        OpenGeminiTableHandle newHandle = handle.withConstraint(newDomain);
        return Optional.of(new ConstraintApplicationResult<>(newHandle, TupleDomain.all(), false));
    }

    @Override
    public Optional<TopNApplicationResult<ConnectorTableHandle>> applyTopN(
            ConnectorSession session,
            ConnectorTableHandle handle,
            long topNCount,
            List<SortItem> sortItems,
            Map<String, ColumnHandle> assignments)
    {
        // only support order by time; and order by time default
        if (sortItems.size() != 1 || !sortItems.get(0).getName().equals("time")) {
            return Optional.empty();
        }
        OpenGeminiTableHandle tableHandle = (OpenGeminiTableHandle) handle;
        if (tableHandle.getLimit() == topNCount && tableHandle.getAscending() == sortItems.get(0).getSortOrder().isAscending()) {
            return Optional.empty();
        }
        OpenGeminiTableHandle newHandle = tableHandle.withLimit(topNCount).withAscending(sortItems.get(0).getSortOrder().isAscending());
        return Optional.of(new TopNApplicationResult<>(newHandle, true, true));
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle, List<ColumnHandle> insertedColumns, RetryMode retryMode)
    {
        if (retryMode != NO_RETRIES) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support query retries");
        }

        List<String> columnNames = insertedColumns.stream().map(handle -> ((OpenGeminiColumnHandle) handle).getColumnName()).toList();
        List<Type> columnTypes = insertedColumns.stream().map(handle -> ((OpenGeminiColumnHandle) handle).getColumnType()).toList();
        List<String> columnKinds = insertedColumns.stream().map(handle -> ((OpenGeminiColumnHandle) handle).getColumnKind()).toList();
        OpenGeminiTableHandle table = (OpenGeminiTableHandle) tableHandle;
        return new OpenGeminiInsertTableHandle(table.getSchemaName(), table.getRpName(), table.getTableName(), columnNames, columnTypes, columnKinds);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(
            ConnectorSession session,
            ConnectorInsertTableHandle insertHandle,
            Collection<Slice> fragments,
            Collection<ComputedStatistics> computedStatistics)
    {
        return Optional.empty();
    }
}

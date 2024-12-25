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

import io.airlift.log.Logger;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.RecordSet;

import javax.inject.Inject;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class OpenGeminiRecordSetProvider
        implements ConnectorRecordSetProvider
{
    private static final Logger log = Logger.get(OpenGeminiRecordSetProvider.class);

    private final OpenGeminiSession session;

    @Inject
    public OpenGeminiRecordSetProvider(OpenGeminiSession session)
    {
        this.session = requireNonNull(session, "session is null");
    }

    @Override
    public RecordSet getRecordSet(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            List<? extends ColumnHandle> columns)
    {
        List<OpenGeminiColumnHandle> columnHandles = columns.stream()
                .map(column -> (OpenGeminiColumnHandle) column)
                .toList();

        OpenGeminiTableHandle tableHandle = (OpenGeminiTableHandle) table;
        String sql = OpenGeminiQLUtils.selectFrom(tableHandle, columnHandles, tableHandle.getConstraint());
        log.info("creating record set, db: %s sql: %s, constraint: %s", tableHandle.getSchemaName(), sql, tableHandle.getConstraint().toString());
        return new OpenGeminiRecordSet(this.session, columnHandles, tableHandle.getSchemaName(), sql);
    }
}

/*
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
package io.trino.plugin.tidb;

import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;
import org.tikv.common.meta.TiTableInfo;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class TiDBMetadata
        implements ConnectorMetadata
{
    private final TiDBClient client;

    @Inject
    public TiDBMetadata(TiDBClient client)
    {
        this.client = requireNonNull(client, "client is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return client.listSchemaNames();
    }

    @Override
    public TiDBTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        return new TiDBTableHandle(tableName.getSchemaName(), tableName.getTableName());
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        return getTableMetadata(((TiDBTableHandle) table).toSchemaTableName());
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> optionalSchemaName)
    {
        return ImmutableList.of();
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        throw new UnsupportedOperationException("Unimplemented");
    }

    public Map<String, ColumnHandle> getColumnHandles(SchemaTableName table)
    {
        TiTableInfo tableInfo = client.getTable(table.getSchemaName(), table.getTableName());
        List<ColumnMetadata> columns = tableInfo.getColumns().stream()
                .map(tidbColumn -> new ColumnMetadata(tidbColumn.getName(), tidbColumn.getType()))
    }

    private ConnectorTableMetadata getTableMetadata(SchemaTableName table)
    {
        throw new UnsupportedOperationException("Unimplemented");
    }
}

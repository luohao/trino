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
package io.trino.plugin.lancedb;

import com.google.inject.Inject;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoInputFile;
import io.trino.lance.LanceDataSource;
import io.trino.lance.LanceReader;
import io.trino.lance.TrinoLanceDataSource;
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.lancedb.LanceDbErrorCode.LANCEDB_BAD_DATA;
import static io.trino.plugin.lancedb.LanceDbErrorCode.LANCEDB_SPLIT_ERROR;
import static io.trino.plugin.lancedb.catalog.FileSystemCatalog.LANCE_SUFFIX;
import static java.util.Objects.requireNonNull;

public class LanceDbPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final TrinoFileSystemFactory fileSystemFactory;
    private final FileFormatDataSourceStats stats;

    @Inject
    public LanceDbPageSourceProvider(
            TrinoFileSystemFactory fileSystemFactory,
            FileFormatDataSourceStats stats)
    {
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.stats = requireNonNull(stats, "stats is null");
    }

    public ConnectorPageSource createPageSource(
            ConnectorSession session,
            Location path,
            List<ColumnHandle> columns)
    {
        if (!path.fileName().endsWith(LANCE_SUFFIX)) {
            throw new TrinoException(LANCEDB_BAD_DATA, "Unsupported file suffix: path=%s".formatted(path.fileName()));
        }
        LanceDataSource lanceDataSource;
        try {
            TrinoFileSystem fileSystem = fileSystemFactory.create(session);
            TrinoInputFile inputFile = fileSystem.newInputFile(path);
            lanceDataSource = new TrinoLanceDataSource(inputFile, stats);
            List<Integer> readColumnIds = columns.stream()
                    .map(LanceDbColumnHandle.class::cast)
                    .map(LanceDbColumnHandle::getId)
                    .collect(toImmutableList());
            LanceReader reader = new LanceReader(lanceDataSource, readColumnIds, Optional.empty());
            return new LanceDbPageSource(reader, lanceDataSource);
        }
        catch (IOException e) {
            throw new TrinoException(LANCEDB_SPLIT_ERROR, e);
        }
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit connectorSplit,
            ConnectorTableHandle table,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter)
    {
        checkArgument(table instanceof LanceDbTableHandle);
        checkArgument(connectorSplit instanceof LanceDbSplit);
        LanceDbSplit split = (LanceDbSplit) connectorSplit;

        return createPageSource(session, Location.of(split.path()), columns);
    }
}

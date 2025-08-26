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
package io.trino.plugin.lance.catalog;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.lance.LanceConfig;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class DirectoryCatalog
        implements TrinoCatalog
{
    public static final String DEFAULT_NAMESPACE = "default";
    // FIXME: move this to a more appropriate package
    public static final String LANCE_SUFFIX = ".lance";
    private final TrinoFileSystemFactory fileSystemFactory;
    private final String warehouseLocation;

    @Inject
    public DirectoryCatalog(TrinoFileSystemFactory fileSystemFactory, LanceConfig config)
    {
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.warehouseLocation = requireNonNull(config.getWarehouseLocation(), "warehouseLocation config is not set");
    }

    private static String directoryName(Location directory, Location location)
    {
        String prefix = directory.path();
        if (!prefix.isEmpty() && !prefix.endsWith("/")) {
            prefix += "/";
        }
        String path = location.path();
        verify(path.endsWith("/"), "path does not end with slash: %s", location);
        verify(path.startsWith(prefix), "path [%s] is not a child of directory [%s]", location, directory);
        return path.substring(prefix.length(), path.length() - 1);
    }

    @Override
    public List<String> listNamespaces(ConnectorSession session)
    {
        return List.of(DEFAULT_NAMESPACE);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> namespace)
    {
        checkArgument(namespace.isEmpty() || namespace.get().equals(DEFAULT_NAMESPACE));
        Location directory = Location.of(warehouseLocation);
        TrinoFileSystem fileSystem = fileSystemFactory.create(session);
        ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();
        try {
            for (Location location : fileSystem.listDirectories(directory)) {
                String directoryName = directoryName(directory, location);
                if (directoryName.endsWith(LANCE_SUFFIX)) {
                    String tableName = directoryName.substring(0, directoryName.length() - LANCE_SUFFIX.length());
                    builder.add(new SchemaTableName(DEFAULT_NAMESPACE, tableName));
                }
            }
        }
        catch (IOException e) {
            throw new RuntimeException(format("Failed to list tables under %s:", warehouseLocation), e);
        }

        return builder.build();
    }

    @Override
    public BaseTable loadTable(ConnectorSession session, SchemaTableName schemaTableName)
    {
        checkArgument(schemaTableName.getSchemaName().equals(DEFAULT_NAMESPACE));
        Location tableLocation = Location.of(warehouseLocation).appendPath(schemaTableName.getTableName() + LANCE_SUFFIX);
        TrinoFileSystem fileSystem = fileSystemFactory.create(session);
        // FIXME: should validate table exists
        return new BaseTable(schemaTableName.getSchemaName(), schemaTableName.getTableName(), fileSystem, tableLocation);
    }
}

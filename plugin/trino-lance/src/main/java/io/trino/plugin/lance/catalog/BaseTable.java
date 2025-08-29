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

import io.airlift.slice.Slice;
import io.trino.filesystem.FileEntry;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputFile;
import io.trino.plugin.lance.metadata.Manifest;
import io.trino.spi.TrinoException;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.plugin.lance.LanceErrorCode.LANCE_INVALID_VERSION_NUMBER;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class BaseTable
{
    // Supports only ManifestNamingScheme::V1
    public static final String VERSIONS_DIR = "_versions";
    public static final String MANIFEST_SUFFIX = ".manifest";
    public static final String LANCE_SUFFIX = ".lance";

    private final String schema;
    private final String name;
    // FIXME: should maybe sue factory instead of filesystem instance
    private final TrinoFileSystem fileSystem;
    private final Location tableLocation;

    public BaseTable(String schema, String name, TrinoFileSystem fileSystem, Location tableLocation)
    {
        this.schema = requireNonNull(schema, "schema is null");
        this.name = requireNonNull(name, "name is null");
        this.fileSystem = requireNonNull(fileSystem, "fileSystem is null");
        this.tableLocation = requireNonNull(tableLocation, "location is null");
    }

    public static long parseManifestVersion(String fileName)
    {
        return Long.parseLong(fileName.substring(0, fileName.length() - MANIFEST_SUFFIX.length()));
    }

    public Location getTableLocation()
    {
        return tableLocation;
    }

    public Manifest loadManifest(Optional<Long> version)
    {
        Optional<Location> manifestLocation = findManifest(version);
        if (manifestLocation.isEmpty()) {
            throw new TrinoException(LANCE_INVALID_VERSION_NUMBER, format("Manifest not found for version: %s", version));
        }
        TrinoInputFile file = fileSystem.newInputFile(manifestLocation.get());
        Slice slice;
        try {
            slice = file.newInput().readFully(0, toIntExact(file.length()));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return Manifest.from(slice);
    }

    private Optional<Location> findManifest(Optional<Long> version)
    {
        long current = -1;
        try {
            FileIterator files = fileSystem.listFiles(tableLocation.appendPath(VERSIONS_DIR));
            while (files.hasNext()) {
                FileEntry file = files.next();
                String fileName = file.location().fileName();
                checkState(
                        fileName.endsWith(MANIFEST_SUFFIX),
                        "Manifest file [%s] does not end with .manifest",
                        file.location().toString());
                long manifestVersion = parseManifestVersion(fileName);
                if (version.isPresent() && manifestVersion > version.get()) {
                    continue;
                }
                if (manifestVersion > current) {
                    current = manifestVersion;
                }
            }
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }

        if (current == -1) {
            return Optional.empty();
        }
        return Optional.of(tableLocation.appendPath(VERSIONS_DIR).appendPath(current + MANIFEST_SUFFIX));
    }

    public String getSchema()
    {
        return schema;
    }

    public String getName()
    {
        return name;
    }
}

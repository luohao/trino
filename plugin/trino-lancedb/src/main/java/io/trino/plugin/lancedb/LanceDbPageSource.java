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

import io.trino.lance.LanceDataSource;
import io.trino.lance.LanceReader;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;

import java.io.IOException;
import java.io.UncheckedIOException;

import static java.util.Objects.requireNonNull;

public class LanceDbPageSource
        implements ConnectorPageSource
{
    private final LanceReader reader;
    private final LanceDataSource dataSource;
    private boolean closed;

    public LanceDbPageSource(
            LanceReader reader,
            LanceDataSource dataSource)
    {
        this.reader = requireNonNull(reader, "reader is null");
        this.dataSource = requireNonNull(dataSource, "dataSource is null");
    }

    @Override
    public long getCompletedBytes()
    {
        return dataSource.getReadBytes();
    }

    @Override
    public long getReadTimeNanos()
    {
        return dataSource.getReadTimeNanos();
    }

    @Override
    public boolean isFinished()
    {
        return closed;
    }

    @Override
    public SourcePage getNextSourcePage()
    {
        SourcePage page = reader.nextSourcePage();
        if (closed || page == null) {
            close();
            return null;
        }
        return page;
    }

    @Override
    public long getMemoryUsage()
    {
        return 0;
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;
        try {
            reader.close();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}

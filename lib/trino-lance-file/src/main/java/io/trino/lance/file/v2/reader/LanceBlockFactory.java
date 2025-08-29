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
package io.trino.lance.file.v2.reader;

import io.trino.spi.block.Block;

import java.io.IOException;
import java.util.function.Function;

// FIXME: enable lazy block
public class LanceBlockFactory
{
    private final Function<Exception, RuntimeException> exceptionTransform;
    private int currentPageId;

    public LanceBlockFactory(Function<Exception, RuntimeException> exceptionTransform)
    {
        this.exceptionTransform = exceptionTransform;
    }

    public void nextPage()
    {
        currentPageId++;
    }

    public interface LanceBlockReader
    {
        Block readBlock()
                throws IOException;
    }
}

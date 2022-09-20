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
package io.trino.testing.tidb;

import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.plugin.tidb.TiDBPlugin;
import io.trino.testing.DistributedQueryRunner;

import static io.trino.testing.TestingSession.testSessionBuilder;

public class TiDBQueryRunnerBuilder
        extends DistributedQueryRunner.Builder<TiDBQueryRunnerBuilder>
{
    private static final Session DEFAULT_SESSION = testSessionBuilder().setSource("test").setCatalog("tidb").setSchema("tiny").build();

    protected TiDBQueryRunnerBuilder()
    {
        super(DEFAULT_SESSION);
    }

    public static TiDBQueryRunnerBuilder builder()
    {
        return new TiDBQueryRunnerBuilder();
    }

    @Override
    public DistributedQueryRunner build()
            throws Exception
    {
        DistributedQueryRunner queryRunner = super.build();
        try {
            ImmutableMap.Builder<String, String> properties = ImmutableMap.builder();
            properties.put("url", "http://127.0.0.1:24000");
            queryRunner.installPlugin(new TiDBPlugin());
            queryRunner.createCatalog("tidb", "tidb", properties.buildOrThrow());
            return queryRunner;
        }
        catch (Exception e) {
            queryRunner.close();
            throw e;
        }
    }

    public static void main(String[] args)
            throws Exception
    {
        DistributedQueryRunner queryRunner = builder().setCoordinatorProperties(ImmutableMap.of("http-server.http.port", "8080")).build();

        Logger log = Logger.get(TiDBQueryRunnerBuilder.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}

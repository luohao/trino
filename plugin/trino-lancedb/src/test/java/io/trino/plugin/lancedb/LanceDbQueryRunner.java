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

import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;

import static io.trino.testing.TestingSession.testSessionBuilder;

public class LanceDbQueryRunner
{
    public static final String LANCEDB_CATALOG = "lancedb";

    private LanceDbQueryRunner() {}

    public static Builder builder()
    {
        return new Builder();
    }

    public static void main(String[] args)
            throws Exception
    {
        QueryRunner queryRunner = LanceDbQueryRunner.builder()
                .addCoordinatorProperty("http-server.http.port", "8080")
                .build();

        Logger log = Logger.get(LanceDbQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }

    public static class Builder
            extends DistributedQueryRunner.Builder<Builder>
    {
        protected Builder()
        {
            super(testSessionBuilder()
                    .setCatalog(LANCEDB_CATALOG)
                    .build());
        }

        @Override
        public DistributedQueryRunner build()
                throws Exception
        {
            DistributedQueryRunner queryRunner = super.build();
            queryRunner.installPlugin(new LanceDbPlugin());
            queryRunner.createCatalog(LANCEDB_CATALOG,
                    "lancedb",
                    ImmutableMap.of("lancedb.catalog.type", "file_system",
                            "lancedb.catalog.warehouse.location", "local:///lance_warehouse/",
                            "fs.native-local.enabled", "true",
                            "local.location", "/Users/hluo/workspace/tmp/"));
            return queryRunner;
        }
    }
}

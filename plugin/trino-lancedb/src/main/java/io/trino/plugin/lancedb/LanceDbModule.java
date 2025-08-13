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

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import io.trino.plugin.lancedb.catalog.CatalogType;
import io.trino.plugin.lancedb.catalog.FileSystemCatalog;
import io.trino.plugin.lancedb.catalog.TrinoCatalog;

import static io.airlift.configuration.ConditionalModule.conditionalModule;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.lancedb.catalog.CatalogType.FILE_SYSTEM;

public class LanceDbModule
        extends AbstractConfigurationAwareModule
{
    @Override
    public void setup(Binder binder)
    {
        binder.bind(LanceDbConnector.class).in(Scopes.SINGLETON);
        binder.bind(LanceDbMetadata.class).in(Scopes.SINGLETON);
        binder.bind(LanceDbSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(LanceDbPageSourceProvider.class).in(Scopes.SINGLETON);
        binder.bind(FileFormatDataSourceStats.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(LanceDbConfig.class);
        bindCatalogModule(FILE_SYSTEM, catalogBinder -> catalogBinder.bind(TrinoCatalog.class).to(FileSystemCatalog.class).in(Scopes.SINGLETON));
    }

    private void bindCatalogModule(CatalogType catalogType, Module module)
    {
        install(conditionalModule(
                LanceDbConfig.class,
                config -> config.getCatalogType() == catalogType,
                module));
    }
}

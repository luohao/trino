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
package io.trino.plugin.lance;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import io.trino.plugin.lance.catalog.DirectoryCatalog;
import io.trino.plugin.lance.catalog.NamespaceType;
import io.trino.plugin.lance.catalog.TrinoCatalog;

import static io.airlift.configuration.ConditionalModule.conditionalModule;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.lance.catalog.NamespaceType.FILE_SYSTEM;

public class LanceModule
        extends AbstractConfigurationAwareModule
{
    @Override
    public void setup(Binder binder)
    {
        binder.bind(LanceConnector.class).in(Scopes.SINGLETON);
        binder.bind(LanceMetadata.class).in(Scopes.SINGLETON);
        binder.bind(LanceSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(LancePageSourceProvider.class).in(Scopes.SINGLETON);
        binder.bind(FileFormatDataSourceStats.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(LanceConfig.class);
        bindCatalogModule(FILE_SYSTEM, catalogBinder -> catalogBinder.bind(TrinoCatalog.class).to(DirectoryCatalog.class).in(Scopes.SINGLETON));
    }

    private void bindCatalogModule(NamespaceType namespaceType, Module module)
    {
        install(conditionalModule(
                LanceConfig.class,
                config -> config.getCatalogType() == namespaceType,
                module));
    }
}

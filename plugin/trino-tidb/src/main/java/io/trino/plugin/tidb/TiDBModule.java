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

import com.google.inject.Binder;
import com.google.inject.Module;

import static com.google.inject.Scopes.SINGLETON;
import static io.airlift.configuration.ConfigBinder.configBinder;

public class TiDBModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(TiDBConnector.class).in(SINGLETON);
        binder.bind(TiDBMetadata.class).in(SINGLETON);
        binder.bind(TiDBSplitManager.class).in(SINGLETON);
        binder.bind(TiDBRecordSetProvider.class).in(SINGLETON);
        configBinder(binder).bindConfig(TiDBConfig.class);
    }
}

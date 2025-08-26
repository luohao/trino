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

import io.airlift.configuration.Config;
import io.trino.plugin.lance.catalog.NamespaceType;
import jakarta.validation.constraints.NotNull;

public class LanceConfig
{
    private NamespaceType namespaceType;
    private String warehouseLocation;

    @NotNull
    public NamespaceType getCatalogType()
    {
        return namespaceType;
    }

    @Config("lance.catalog.type")
    public LanceConfig setCatalogType(NamespaceType namespaceType)
    {
        this.namespaceType = namespaceType;
        return this;
    }

    public String getWarehouseLocation()
    {
        return warehouseLocation;
    }

    @Config("lance.catalog.warehouse.location")
    public LanceConfig setWarehouseLocation(String warehouseLocation)
    {
        this.warehouseLocation = warehouseLocation;
        return this;
    }
}

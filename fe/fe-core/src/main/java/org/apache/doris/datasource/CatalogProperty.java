// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.datasource;

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Resource;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.property.PropertyConverter;
import org.apache.doris.datasource.property.storage.StorageProperties;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import lombok.Data;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * CatalogProperty to store the properties for catalog.
 * the properties in "properties" will overwrite properties in "resource"
 */
@Data
public class CatalogProperty {
    private static final Logger LOG = LogManager.getLogger(CatalogProperty.class);

    @SerializedName(value = "resource")
    private String resource;
    @SerializedName(value = "properties")
    private Map<String, String> properties;

    private volatile Map<StorageProperties.Type, StorageProperties> storagePropertiesMap;

    private volatile Resource catalogResource = null;

    public CatalogProperty(String resource, Map<String, String> properties) {
        this.resource = Strings.nullToEmpty(resource);
        this.properties = properties;
        if (this.properties == null) {
            this.properties = Maps.newConcurrentMap();
        }
    }

    private Resource catalogResource() {
        if (!Strings.isNullOrEmpty(resource) && catalogResource == null) {
            synchronized (this) {
                if (catalogResource == null) {
                    catalogResource = Env.getCurrentEnv().getResourceMgr().getResource(resource);
                }
            }
        }
        return catalogResource;
    }

    public String getOrDefault(String key, String defaultVal) {
        String val = properties.get(key);
        if (val == null) {
            Resource res = catalogResource();
            if (res != null) {
                val = res.getCopiedProperties().getOrDefault(key, defaultVal);
            } else {
                val = defaultVal;
            }
        }
        return val;
    }

    public Map<String, String> getProperties() {
        Map<String, String> mergedProperties = Maps.newHashMap();
        if (!Strings.isNullOrEmpty(resource)) {
            Resource res = catalogResource();
            if (res != null) {
                mergedProperties = res.getCopiedProperties();
            }
        }
        mergedProperties.putAll(properties);
        return mergedProperties;
    }

    public void modifyCatalogProps(Map<String, String> props) {
        properties.putAll(PropertyConverter.convertToMetaProperties(props));
        this.storagePropertiesMap = null;
    }

    private void reInitCatalogStorageProperties() {
        this.storagePropertiesMap = new HashMap<>();
        List<StorageProperties> storageProperties;
        try {
            storageProperties = StorageProperties.createAll(this.properties);
            this.storagePropertiesMap.putAll(storageProperties.stream()
                    .collect(java.util.stream.Collectors.toMap(StorageProperties::getType, Function.identity())));
        } catch (UserException e) {
            throw new RuntimeException(e);
        }

    }

    public void rollBackCatalogProps(Map<String, String> props) {
        properties.clear();
        properties = new HashMap<>(props);
        this.storagePropertiesMap = null;
    }


    public Map<String, String> getHadoopProperties() {
        Map<String, String> hadoopProperties = getProperties();
        hadoopProperties.putAll(PropertyConverter.convertToHadoopFSProperties(getProperties()));
        return hadoopProperties;
    }

    public void addProperty(String key, String val) {
        this.properties.put(key, val);
        this.storagePropertiesMap = null; // reset storage properties map
    }

    public void deleteProperty(String key) {
        this.properties.remove(key);
        this.storagePropertiesMap = null;
    }

    public Map<StorageProperties.Type, StorageProperties> getStoragePropertiesMap() {
        if (storagePropertiesMap == null) {
            synchronized (this) {
                if (storagePropertiesMap == null) {
                    reInitCatalogStorageProperties();
                }
            }
        }
        return storagePropertiesMap;
    }
}

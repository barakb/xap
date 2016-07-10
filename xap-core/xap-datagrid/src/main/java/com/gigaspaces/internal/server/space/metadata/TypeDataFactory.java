/*
 * Copyright (c) 2008-2016, GigaSpaces Technologies, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.gigaspaces.internal.server.space.metadata;

import com.gigaspaces.internal.server.metadata.IServerTypeDesc;
import com.gigaspaces.internal.server.space.SpaceConfigReader;
import com.gigaspaces.metadata.index.ISpaceIndex;
import com.j_spaces.core.cache.CacheManager;
import com.j_spaces.core.cache.CompoundCustomTypeDataIndex;
import com.j_spaces.core.cache.CompoundIndexSegmentTypeData;
import com.j_spaces.core.cache.CustomMultiValueTypeDataIndex;
import com.j_spaces.core.cache.CustomTypeDataIndex;
import com.j_spaces.core.cache.MultiValueTypeDataIndex;
import com.j_spaces.core.cache.TypeData;
import com.j_spaces.core.cache.TypeDataIndex;

import static com.j_spaces.core.Constants.CacheManager.CACHE_MANAGER_USE_ECONOMY_HASHMAP_DEFAULT;
import static com.j_spaces.core.Constants.CacheManager.CACHE_MANAGER_USE_ECONOMY_HASHMAP_PROP;

@com.gigaspaces.api.InternalApi
public class TypeDataFactory {
    private final boolean _useEconomyHashMap;

    private final CacheManager _cacheManager;

    public TypeDataFactory(SpaceConfigReader configReader, CacheManager cacheManager) {
        this._useEconomyHashMap = configReader.getBooleanSpaceProperty(
                CACHE_MANAGER_USE_ECONOMY_HASHMAP_PROP, CACHE_MANAGER_USE_ECONOMY_HASHMAP_DEFAULT);

        configReader.assertSpacePropertyNotExists("engine.extended-match.enabled-classes", "7.0.1", "8.0");
        _cacheManager = cacheManager;
    }

    public boolean useEconomyHashMap() {
        return _useEconomyHashMap;
    }

    public CacheManager getCcheManager() {
        return _cacheManager;
    }

    public TypeData createTypeData(IServerTypeDesc serverTypeDesc) {
        return new TypeData(serverTypeDesc, this, _cacheManager.getEngine().isLocalCache(), _cacheManager.isResidentEntriesCachePolicy());
    }

    /*
     * one or more indexes have been created- create a new type data
     * given the original one and the new server=type-descriptor
     * @param reason = one of TypeData reason static attributes 
     * 
     */
    public TypeData createTypeDataOnDynamicIndexCreation(IServerTypeDesc serverTypeDesc, TypeData originalTypeData, TypeData.TypeDataRecreationReasons reason) {
        return new TypeData(serverTypeDesc, originalTypeData, reason);
    }

    public <K> TypeDataIndex<K> createTypeDataIndex(CacheManager cacheManager, ISpaceIndex index, int pos, int indexCreationNumber, Class<?> indexValueClass, ISpaceIndex.FifoGroupsIndexTypes fifoGroupsIndexType) {
        return new TypeDataIndex<K>(cacheManager, index, pos, _useEconomyHashMap, indexCreationNumber, indexValueClass, fifoGroupsIndexType);
    }

    public <K> TypeDataIndex<K> createMultiValuePerEntryTypeDataIndex(CacheManager cacheManager, ISpaceIndex index, int pos, int indexCreationNumber, ISpaceIndex.MultiValuePerEntryIndexTypes indexType) {
        return new MultiValueTypeDataIndex<K>(cacheManager, index, pos, _useEconomyHashMap, indexCreationNumber, indexType);

    }


    public <K> TypeDataIndex<K> createCustomTypeDataIndex(CacheManager cacheManager, ISpaceIndex index, int pos, int indexCreationNumber, ISpaceIndex.FifoGroupsIndexTypes fifoGroupsIndexType) {
        return new CustomTypeDataIndex<K>(cacheManager, index, pos, indexCreationNumber, fifoGroupsIndexType);
    }

    public <K> TypeDataIndex<K> createCustomMultiValuePerEntryTypeDataIndex(CacheManager cacheManager, ISpaceIndex index, int pos, int indexCreationNumber, ISpaceIndex.MultiValuePerEntryIndexTypes indexType) {
        return new CustomMultiValueTypeDataIndex<K>(cacheManager, index, pos, indexCreationNumber, indexType);
    }

    public <K> TypeDataIndex<K> createCompoundCustomTypeDataIndex(CacheManager cacheManager, ISpaceIndex index, CompoundIndexSegmentTypeData[] segments, int indexCreationNumber, int indexPosition, ISpaceIndex.FifoGroupsIndexTypes fifoGroupsIndexType) {
        return new CompoundCustomTypeDataIndex<K>(cacheManager, index, segments, indexCreationNumber, indexPosition, fifoGroupsIndexType);
    }
}

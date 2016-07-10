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


package org.openspaces.core.map;

import com.j_spaces.core.IJSpace;
import com.j_spaces.javax.cache.EvictionStrategy;
import com.j_spaces.map.IMap;

/**
 * A simple configurer helper to create {@link IMap} based on an {@link IJSpace}. The configurer
 * wraps {@link org.openspaces.core.map.MapFactoryBean} and providing a simpler means to configure
 * it using code.
 *
 * <p>An example of using it:
 * <pre>
 * UrlSpaceConfigurer urlSpaceConfigurer = new UrlSpaceConfigurer("/./space").schema("persistent")
 *          .noWriteLeaseMode(true).lookupGroups(new String[] {"kimchy"});
 * IJSpace space = urlSpaceConfigurer.space();
 *
 * IMap map = new MapConfigurer(space).localCachePutFirst(true).createMap();
 * ...
 * urlSpaceConfigurer.destroySpace(); // optional
 * </pre>
 *
 * @author kimchy
 */
public class MapConfigurer {

    public static enum UpdateMode {
        PULL,
        PUSH
    }

    private MapFactoryBean mapFactoryBean;

    private IMap map;

    private LocalCacheSupport localCacheSupport;

    public MapConfigurer(IJSpace space) {
        mapFactoryBean = new MapFactoryBean();
        mapFactoryBean.setSpace(space);
    }

    /**
     * @see org.openspaces.core.map.MapFactoryBean#setClustered(Boolean)
     */
    public MapConfigurer clustered(boolean clustered) {
        mapFactoryBean.setClustered(clustered);
        return this;
    }

    /**
     * @see org.openspaces.core.map.MapFactoryBean#setCompression(int)
     */
    public MapConfigurer compression(int compression) {
        mapFactoryBean.setCompression(compression);
        return this;
    }

    /**
     * If no local cache properties are set, will mark this map to use local cache.
     */
    public MapConfigurer useLocalCache() {
        if (localCacheSupport == null) {
            localCacheSupport = new LocalCacheSupport();
        }
        return this;
    }

    /**
     * @see org.openspaces.core.map.MapFactoryBean#setLocalCacheSupport(LocalCacheSupport)
     * @see org.openspaces.core.map.LocalCacheSupport#setVersioned(boolean)
     */
    public MapConfigurer localCacheVersioned(boolean versioned) {
        if (localCacheSupport == null) {
            localCacheSupport = new LocalCacheSupport();
        }
        localCacheSupport.setVersioned(versioned);
        return this;
    }

    /**
     * @see org.openspaces.core.map.MapFactoryBean#setLocalCacheSupport(LocalCacheSupport)
     * @see org.openspaces.core.map.LocalCacheSupport#setEvictionStrategy(com.j_spaces.javax.cache.EvictionStrategy)
     */
    public MapConfigurer localCacheEvictionStrategy(EvictionStrategy evictionStrategy) {
        if (localCacheSupport == null) {
            localCacheSupport = new LocalCacheSupport();
        }
        localCacheSupport.setEvictionStrategy(evictionStrategy);
        return this;
    }

    /**
     * @see org.openspaces.core.map.MapFactoryBean#setLocalCacheSupport(LocalCacheSupport)
     * @see org.openspaces.core.map.LocalCacheSupport#setUpdateModeName(String)
     */
    public MapConfigurer localCacheUpdateMode(UpdateMode updateMode) {
        if (localCacheSupport == null) {
            localCacheSupport = new LocalCacheSupport();
        }
        if (updateMode == UpdateMode.PULL) {
            localCacheSupport.setUpdateModeName(LocalCacheSupport.LOCAL_CACHE_UPDATE_MODE_PULL);
        } else if (updateMode == UpdateMode.PUSH) {
            localCacheSupport.setUpdateModeName(LocalCacheSupport.LOCAL_CACHE_UPDATE_MODE_PUSH);
        }
        return this;
    }

    /**
     * @see org.openspaces.core.map.MapFactoryBean#setLocalCacheSupport(LocalCacheSupport)
     * @see org.openspaces.core.map.LocalCacheSupport#setPutFirst(boolean)
     */
    public MapConfigurer localCachePutFirst(boolean putFirst) {
        if (localCacheSupport == null) {
            localCacheSupport = new LocalCacheSupport();
        }
        localCacheSupport.setPutFirst(putFirst);
        return this;
    }

    /**
     * @see org.openspaces.core.map.MapFactoryBean#setLocalCacheSupport(LocalCacheSupport)
     * @see org.openspaces.core.map.LocalCacheSupport#setSizeLimit(int)
     */
    public MapConfigurer localCacheSizeLimit(int sizeLimit) {
        if (localCacheSupport == null) {
            localCacheSupport = new LocalCacheSupport();
        }
        localCacheSupport.setSizeLimit(sizeLimit);
        return this;
    }

    /**
     * Creates an {@link com.j_spaces.map.IMap} based on the configuration. Uses {@link
     * MapFactoryBean#afterPropertiesSet()}.
     */
    public IMap createMap() {
        if (map == null) {
            if (localCacheSupport != null) {
                mapFactoryBean.setLocalCacheSupport(localCacheSupport);
            }
            mapFactoryBean.afterPropertiesSet();
            map = (IMap) mapFactoryBean.getObject();
        }
        return map;
    }

    /**
     * Creates an {@link com.j_spaces.map.IMap} based on the configuration. Uses {@link
     * MapFactoryBean#afterPropertiesSet()}.
     */
    public IMap map() {
        return createMap();
    }
}

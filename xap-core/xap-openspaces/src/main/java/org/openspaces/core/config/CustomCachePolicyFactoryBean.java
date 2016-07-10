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

package org.openspaces.core.config;

import com.gigaspaces.server.eviction.SpaceEvictionStrategy;

import org.openspaces.core.space.CachePolicy;
import org.openspaces.core.space.CustomCachePolicy;
import org.springframework.beans.factory.InitializingBean;

/**
 * A factory for creating {@link CustomCachePolicy} instance.
 *
 * @author idan
 * @since 9.1
 */
public class CustomCachePolicyFactoryBean implements InitializingBean {

    private Integer size;
    private Integer initialLoadPercentage;
    private SpaceEvictionStrategy spaceEvictionStrategy;

    public Integer getSize() {
        return size;
    }

    public void setSize(Integer size) {
        this.size = size;
    }

    public Integer getInitialLoadPercentage() {
        return initialLoadPercentage;
    }

    public void setInitialLoadPercentage(Integer initialLoadPercentage) {
        this.initialLoadPercentage = initialLoadPercentage;
    }

    public SpaceEvictionStrategy getSpaceEvictionStrategy() {
        return spaceEvictionStrategy;
    }

    public void setSpaceEvictionStrategy(SpaceEvictionStrategy spaceEvictionStrategy) {
        this.spaceEvictionStrategy = spaceEvictionStrategy;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
    }

    public CachePolicy asCachePolicy() {
        final CustomCachePolicy policy = new CustomCachePolicy();
        if (size != null)
            policy.setSize(size);
        if (initialLoadPercentage != null)
            policy.setInitialLoadPercentage(initialLoadPercentage);
        policy.setEvictionStrategy(spaceEvictionStrategy);
        return policy;
    }


}

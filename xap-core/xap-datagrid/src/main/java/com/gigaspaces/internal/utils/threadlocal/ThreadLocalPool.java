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

package com.gigaspaces.internal.utils.threadlocal;

import java.util.ArrayList;

/**
 * ThreadLocal based resource pool.
 *
 * @param <T> resource type
 * @author guy
 * @since 7.0.1
 */
@com.gigaspaces.api.InternalApi
public class ThreadLocalPool<T extends AbstractResource> {

    final private ThreadLocal<ArrayList<T>> _SLHolderPool;
    final private PoolFactory<T> _factory;

    public ThreadLocalPool(PoolFactory<T> factory) {
        _factory = factory;
        _SLHolderPool = new ThreadLocal<ArrayList<T>>() {
            @Override
            protected ArrayList<T> initialValue() {
                return new ArrayList<T>();
            }
        };
    }

    public T get() {
        T resource = null;
        ArrayList<T> list = _SLHolderPool.get();
        for (int i = 0; i < list.size(); ++i) {
            resource = list.get(i);
            if (resource.isAvaliable()) {
                resource.setAvaliable(false);
                return resource;
            }
        }
        resource = _factory.create(); // created unAvaliable
        list.add(resource);
        return resource;
    }
}

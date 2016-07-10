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

package com.gigaspaces.query.extension.impl;

import com.gigaspaces.query.extension.QueryExtensionProvider;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Niv Ingberg
 * @since 11.0
 */
@com.gigaspaces.api.InternalApi
public class QueryExtensionProviderCache {
    private static final Map<String, QueryExtensionProvider> cache = new ConcurrentHashMap<String, QueryExtensionProvider>();

    public static QueryExtensionProvider getByClass(Class<? extends QueryExtensionProvider> providerClass) {
        QueryExtensionProvider result = cache.get(providerClass.getName());
        if (result == null) {
            synchronized (cache) {
                result = cache.get(providerClass.getName());
                if (result == null) {
                    try {
                        result = providerClass.newInstance();
                        cache.put(providerClass.getName(), result);
                    } catch (InstantiationException e) {
                        throw new IllegalStateException("Failed to create SpaceQueryExtensionProvider from " + providerClass.getName(), e);
                    } catch (IllegalAccessException e) {
                        throw new IllegalStateException("Failed to create SpaceQueryExtensionProvider from " + providerClass.getName(), e);
                    }
                }
            }
        }
        return result;
    }
}

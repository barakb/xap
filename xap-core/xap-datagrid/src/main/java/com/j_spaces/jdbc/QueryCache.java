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

package com.j_spaces.jdbc;

import com.gigaspaces.internal.utils.collections.ConcurrentBoundedCache;
import com.gigaspaces.internal.utils.collections.ConcurrentSoftCache;
import com.j_spaces.kernel.SystemProperties;

import java.util.Map;


/**
 * Caches JDBC queries by their string representation
 *
 * @author anna
 * @since 6.1
 */
@com.gigaspaces.api.InternalApi
public class QueryCache {
    /**
     *
     */
    private Map<String, Query> _statementCache;

    /**
     *
     */
    public QueryCache() {
        String val = System.getProperty(SystemProperties.ENABLE_BOUNDED_QUERY_CACHE);
        boolean isCacheBounded = new Boolean(val != null ? val : SystemProperties.ENABLE_BOUNDED_QUERY_CACHE_DEFAULT);
        _statementCache = isCacheBounded ? new ConcurrentBoundedCache<String, Query>() : new ConcurrentSoftCache<String, Query>();
    }

    public void addQueryToCache(String statement, Query query) {
        _statementCache.put(statement, query);
    }

    // return the query from the cache, it may be null though, so the caller
    // method should check
    public Query getQueryFromCache(String statement) {
        return _statementCache.get(statement);
    }


    /**
     *
     */
    public void clear() {
        _statementCache.clear();

    }


}
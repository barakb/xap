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

package com.gigaspaces.internal.query;

import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.server.ServerEntry;
import com.j_spaces.core.cache.CacheManager;
import com.j_spaces.core.client.SQLQuery;

import java.util.List;

/**
 * Represents a custom query interface.
 *
 * @author Niv Ingberg
 * @since 7.1
 */
public interface ICustomQuery {
    /**
     * @return the list of query defined indexes. If no index is defined - empty list is returned
     */
    List<IQueryIndexScanner> getCustomIndexes();

    /**
     * Checks whether the specified entry matches this query.
     *
     * @return Returns true if the specified entry matches this query, false otherwise.
     */
    boolean matches(CacheManager cacheManager, ServerEntry entry, String skipAlreadyMatchedIndexPath);

    /**
     * @param typeDesc TODO
     * @return SQLQuery
     */
    SQLQuery toSQLQuery(ITypeDesc typeDesc);
}

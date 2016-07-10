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

/**
 *
 */
package com.gigaspaces.internal.query;

import com.j_spaces.core.cache.TypeData;
import com.j_spaces.core.cache.TypeDataIndex;
import com.j_spaces.core.cache.context.Context;
import com.j_spaces.kernel.list.IObjectsList;

/**
 * Scans only the entries that have a null index value
 *
 * @author anna
 * @since 7.1
 */
@com.gigaspaces.api.InternalApi
public class NullValueIndexScanner
        extends AbstractQueryIndex {
    private static final long serialVersionUID = 7745966844359780107L;

    /**
     *
     */
    public NullValueIndexScanner() {
        super();

    }

    /**
     * @param indexName
     * @param indexValue
     */
    public NullValueIndexScanner(String indexName) {
        super(indexName);

    }

    @Override
    protected IObjectsList getEntriesByIndex(Context context, TypeData typeData, TypeDataIndex<Object> index, boolean fifoGroupsScan) {
        return index.getNullEntries();
    }

    public boolean requiresOrderedIndex() {
        return false;
    }

    @Override
    protected boolean hasIndexValue() {
        return false;
    }

    /* (non-Javadoc)
     * @see com.gigaspaces.internal.query.IQueryIndexScanner#getIndexValue()
     */
    public Object getIndexValue() {
        return null;
    }

    /**
     * equality matching preserves fifo order
     */
    @Override
    public boolean supportsFifoOrder() {
        return true;
    }

    public boolean supportsTemplateIndex() {
        return true;
    }

    @Override
    public boolean requiresValueForIndexSearch() {
        return false;
    }

}

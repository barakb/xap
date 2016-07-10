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

/**
 * Factory for creating custom query index scanners
 *
 * @author anna
 * @since 7.1
 */
@com.gigaspaces.api.InternalApi
public class QueryIndexes {

    public static IQueryIndexScanner newNullValueIndexScanner(String indexName) {
        return new NullValueIndexScanner(indexName);
    }


    public static IQueryIndexScanner newIndexScanner(String indexName, Object indexValue) {
        if (indexValue == null)
            return new NullValueIndexScanner(indexName);

        return new ExactValueIndexScanner(indexName, indexValue);
    }

    public static IQueryIndexScanner newRangeIndexScanner(String indexName, Comparable min,
                                                          boolean includeMin, Comparable max, boolean includeMax) {

        return new RangeIndexScanner(indexName, min, includeMin, max, includeMax);
    }

    public static IQueryIndexScanner newAscendingIndexScanner(String indexName, Comparable from) {
        if (from == null)
            throw new IllegalArgumentException("Illegal index - 'from' parameter can not be null");
        return new RangeIndexScanner(indexName, from, true, null, true);
    }


    public static IQueryIndexScanner newDescendingIndexScanner(String indexName, Comparable from) {
        if (from == null)
            throw new IllegalArgumentException("Illegal index - 'from' parameter can not be null");

        return new RangeIndexScanner(indexName, null, true, from, true);
    }


}

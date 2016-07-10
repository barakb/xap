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


package com.gigaspaces.client;

import java.util.Iterator;

/**
 * Holds iterable results of the <code>takeByIds</code> operation. When iterating through the
 * results, null values are skipped. If you want to access null values, use the {@link
 * #getResultsArray()} method. Results are ordered based on the list of IDs provided to the
 * <code>takeByIds</code> method.
 *
 * @author idan
 * @since 7.1.1
 */
public interface TakeByIdsResult<T> extends Iterable<T> {
    /**
     * Returns an iterator over the takeByIds operation results.
     */
    public Iterator<T> iterator();

    /**
     * Returns the results array for the <code>takeByIds</code> operation. <p>The array's size is
     * the same as that of the given Ids array. Unmatched Ids will have a <code>null</code> value in
     * the corresponding results array index.
     *
     * @return Results array.
     */
    public T[] getResultsArray();

}

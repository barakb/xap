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


package org.openspaces.events.polling.receive;

import com.j_spaces.core.client.ReadModifiers;

import org.openspaces.core.GigaSpace;
import org.springframework.dao.DataAccessException;

/**
 * First tries and perform a {@link org.openspaces.core.GigaSpace#readMultiple(Object, int)} using
 * the provided template and configured maxEntries (defaults to <code>50</code>). If no values are
 * returned, will perform a blocking read operation using {@link org.openspaces.core.GigaSpace#read(Object,
 * long)}.
 *
 * @author kimchy
 */
public class MultiReadReceiveOperationHandler extends AbstractMemoryOnlySearchReceiveOperationHandler {

    private static final int DEFAULT_MAX_ENTRIES = 50;

    private int maxEntries = DEFAULT_MAX_ENTRIES;

    /**
     * Sets the max entries the initial take multiple operation will perform.
     */
    public void setMaxEntries(int maxEntries) {
        this.maxEntries = maxEntries;
    }

    /**
     * First tries and perform a {@link org.openspaces.core.GigaSpace#readMultiple(Object, int)}
     * using the provided template and configured maxEntries (defaults to <code>50</code>). If no
     * values are returned, will perform a blocking read operation using {@link
     * org.openspaces.core.GigaSpace#read(Object, long)}.
     */
    @Override
    protected Object doReceiveBlocking(Object template, GigaSpace gigaSpace, long receiveTimeout) throws DataAccessException {
        int modifiers = gigaSpace.getSpace().getReadModifiers();
        if (useMemoryOnlySearch)
            modifiers |= ReadModifiers.MEMORY_ONLY_SEARCH;
        Object[] results = gigaSpace.readMultiple(template, maxEntries, modifiers);
        if (results != null && results.length > 0) {
            return results;
        }
        return gigaSpace.read(template, receiveTimeout, modifiers);
    }

    /**
     * Perform a {@link org.openspaces.core.GigaSpace#readMultiple(Object, int)} using the provided
     * template and configured maxEntries (defaults to <code>50</code>). This is a non blocking
     * operation.
     */
    @Override
    protected Object doReceiveNonBlocking(Object template, GigaSpace gigaSpace) throws DataAccessException {
        int modifiers = gigaSpace.getSpace().getReadModifiers();
        if (useMemoryOnlySearch)
            modifiers |= ReadModifiers.MEMORY_ONLY_SEARCH;
        Object[] results = gigaSpace.readMultiple(template, maxEntries, modifiers);
        if (results != null && results.length > 0) {
            return results;
        }
        return null;
    }

    @Override
    public String toString() {
        return "Multi Read, maxEntries[" + maxEntries + "], nonBlocking[" + nonBlocking + "], nonBlockingFactor[" + nonBlockingFactor
                + "], useMemoryOnlySearch[" + isUseMemoryOnlySearch() + "]";
    }
}

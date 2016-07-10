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

import com.j_spaces.core.client.TakeModifiers;

import org.openspaces.core.GigaSpace;
import org.springframework.dao.DataAccessException;

/**
 * First tries and perform a {@link org.openspaces.core.GigaSpace#takeMultiple(Object, int, int)}
 * using the provided template, the configured maxEntries (defaults to <code>50</code>) and the
 * configured fifoGroups (default to <code>false</code>). <p>If no values are returned, will perform
 * a blocking take operation using {@link org.openspaces.core.GigaSpace#take(Object, long, int)}.
 *
 * @author kimchy
 */
public class MultiTakeReceiveOperationHandler extends AbstractFifoGroupingReceiveOperationHandler {

    private static final int DEFAULT_MAX_ENTRIES = 50;

    private int maxEntries = DEFAULT_MAX_ENTRIES;

    /**
     * Sets the max entries the initial take multiple operation will perform.
     */
    public void setMaxEntries(int maxEntries) {
        this.maxEntries = maxEntries;
    }

    /**
     * First tries and perform a {@link org.openspaces.core.GigaSpace#takeMultiple(Object, int,
     * int)} using the provided template, the configured maxEntries (defaults to <code>50</code>)
     * and the configured fifoGroups (default to <code>false</code>). If no values are returned,
     * will perform a blocking take operation using {@link org.openspaces.core.GigaSpace#take(Object,
     * long, int)}.
     */
    @Override
    protected Object doReceiveBlocking(Object template, GigaSpace gigaSpace, long receiveTimeout) throws DataAccessException {
        int modifiers = gigaSpace.getSpace().getReadModifiers();
        if (useFifoGrouping)
            modifiers |= TakeModifiers.FIFO_GROUPING_POLL;
        if (useMemoryOnlySearch)
            modifiers |= TakeModifiers.MEMORY_ONLY_SEARCH;

        Object[] results = gigaSpace.takeMultiple(template, maxEntries, modifiers);
        if (results != null && results.length > 0) {
            return results;
        }
        return gigaSpace.take(template, receiveTimeout, modifiers);
    }

    /**
     * Performs a non blocking {@link org.openspaces.core.GigaSpace#takeMultiple(Object, int, int)}
     * using the provided template, the configured maxEntries (defaults to <code>50</code>) and the
     * configured fifoGroups (default to <code>false</code>).
     */
    @Override
    protected Object doReceiveNonBlocking(Object template, GigaSpace gigaSpace) throws DataAccessException {
        int modifiers = gigaSpace.getSpace().getReadModifiers();
        if (useFifoGrouping)
            modifiers |= TakeModifiers.FIFO_GROUPING_POLL;
        if (useMemoryOnlySearch)
            modifiers |= TakeModifiers.MEMORY_ONLY_SEARCH;
        Object[] results = gigaSpace.takeMultiple(template, maxEntries, modifiers);
        if (results != null && results.length > 0) {
            return results;
        }
        return null;
    }

    @Override
    public String toString() {
        return "Multi Take, maxEntries[" + maxEntries + "], nonBlocking[" + nonBlocking + "], nonBlockingFactor[" + nonBlockingFactor
                + "], useFifoGroups[" + isUseFifoGrouping() + "], useMemoryOnlySearch[" + isUseMemoryOnlySearch() + "]";
    }
}

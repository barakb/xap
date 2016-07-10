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

import com.gigaspaces.client.ReadModifiers;

import org.openspaces.core.GigaSpace;
import org.springframework.dao.DataAccessException;

/**
 * First tries and perform a {@link org.openspaces.core.GigaSpace#readMultiple(Object, int, int)}
 * using the provided template, configured maxEntries (defaults to <code>50</code>) and the
 * configured fifoGroups (default to <code>false</code>). <p>If no values are returned, will perform
 * a blocking read operation using {@link org.openspaces.core.GigaSpace#read(Object, long, int)}.
 *
 * <p>Read operations are performed under an exclusive read lock which mimics the similar behavior
 * as take without actually taking the entry from the space.
 *
 * @author kimchy
 */
public class MultiExclusiveReadReceiveOperationHandler extends AbstractFifoGroupingReceiveOperationHandler {

    private static final int DEFAULT_MAX_ENTRIES = 50;

    private int maxEntries = DEFAULT_MAX_ENTRIES;

    /**
     * Sets the max entries the initial take multiple operation will perform.
     */
    public void setMaxEntries(int maxEntries) {
        this.maxEntries = maxEntries;
    }

    /**
     * First tries and perform a {@link org.openspaces.core.GigaSpace#readMultiple(Object, int,
     * int)} using the provided template, configured maxEntries (defaults to <code>50</code>) and
     * the configured fifoGroups (default to <code>false</code>). If no values are returned, will
     * perform a blocking read operation using {@link org.openspaces.core.GigaSpace#read(Object,
     * long, int)}.
     *
     * <p>Read operations are performed under an exclusive read lock which mimics the similar
     * behavior as take without actually taking the entry from the space.
     */
    @Override
    protected Object doReceiveBlocking(Object template, GigaSpace gigaSpace, long receiveTimeout) throws DataAccessException {
        ReadModifiers modifiers = gigaSpace.getDefaultReadModifiers().add(ReadModifiers.EXCLUSIVE_READ_LOCK);
        if (useFifoGrouping)
            modifiers = modifiers.add(ReadModifiers.FIFO_GROUPING_POLL);
        if (useMemoryOnlySearch)
            modifiers = modifiers.add(ReadModifiers.MEMORY_ONLY_SEARCH);

        Object[] results = gigaSpace.readMultiple(template, maxEntries, modifiers);
        if (results != null && results.length > 0) {
            return results;
        }
        return gigaSpace.read(template, receiveTimeout, modifiers);
    }

    /**
     * Perform a {@link org.openspaces.core.GigaSpace#readMultiple(Object, int, int)} using the
     * provided template, configured maxEntries (defaults to <code>50</code>) and the configured
     * fifoGroups (default to <code>false</code>).
     *
     * <p>Read operations are performed under an exclusive read lock which mimics the similar
     * behavior as take without actually taking the entry from the space.
     */
    @Override
    protected Object doReceiveNonBlocking(Object template, GigaSpace gigaSpace) throws DataAccessException {
        ReadModifiers modifiers = gigaSpace.getDefaultReadModifiers().add(ReadModifiers.EXCLUSIVE_READ_LOCK);
        if (useFifoGrouping)
            modifiers = modifiers.add(ReadModifiers.FIFO_GROUPING_POLL);
        if (useMemoryOnlySearch)
            modifiers = modifiers.add(ReadModifiers.MEMORY_ONLY_SEARCH);

        Object[] results = gigaSpace.readMultiple(template, maxEntries, modifiers);
        if (results != null && results.length > 0) {
            return results;
        }
        return null;
    }

    @Override
    public String toString() {
        return "Multi Exclusive Read, maxEntries[" + maxEntries + "], nonBlocking[" + nonBlocking + "], nonBlockingFactor[" + nonBlockingFactor
                + "], useFifoGroups[" + isUseFifoGrouping() + "], useMemoryOnlySearch[" + isUseMemoryOnlySearch() + "]";
    }
}
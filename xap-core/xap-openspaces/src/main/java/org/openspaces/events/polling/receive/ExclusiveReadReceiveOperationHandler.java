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
 * Performs single read operation using {@link org.openspaces.core.GigaSpace#read(Object, long,
 * int)} under an exclusive read lock. This receive operation handler allows to lock entries so
 * other receive operations won't be able to obtain it (mimics the take operation) but without
 * actually performing a take from the Space.
 *
 * Note, this receive operation handler must be performed under a transaction.
 *
 * @author kimchy
 */
public class ExclusiveReadReceiveOperationHandler extends AbstractFifoGroupingReceiveOperationHandler {

    /**
     * Performs single read operation using {@link org.openspaces.core.GigaSpace#read(Object, long,
     * int)} under an exclusive read lock. This receive operation handler allows to lock entries so
     * other receive operations won't be able to obtain it (mimics the take operation) but without
     * actually performing a take from the Space.
     *
     * Note, this receive operation handler must be performed under a transaction.
     */
    @Override
    protected Object doReceiveBlocking(Object template, GigaSpace gigaSpace, long receiveTimeout) throws DataAccessException {
        ReadModifiers modifiers = gigaSpace.getDefaultReadModifiers().add(ReadModifiers.EXCLUSIVE_READ_LOCK);
        if (useFifoGrouping)
            modifiers = modifiers.add(ReadModifiers.FIFO_GROUPING_POLL);
        if (useMemoryOnlySearch)
            modifiers = modifiers.add(ReadModifiers.MEMORY_ONLY_SEARCH);
        return gigaSpace.read(template, receiveTimeout, modifiers);
    }

    /**
     * Performs single read operation using {@link org.openspaces.core.GigaSpace#read(Object, long,
     * int)} under an exclusive read lock with no timeout. This receive operation handler allows to
     * lock entries so other receive operations won't be able to obtain it (mimics the take
     * operation) but without actually performing a take from the Space.
     *
     * Note, this receive operation handler must be performed under a transaction.
     */
    @Override
    protected Object doReceiveNonBlocking(Object template, GigaSpace gigaSpace) throws DataAccessException {
        ReadModifiers modifiers = gigaSpace.getDefaultReadModifiers().add(ReadModifiers.EXCLUSIVE_READ_LOCK);
        if (useFifoGrouping)
            modifiers = modifiers.add(ReadModifiers.FIFO_GROUPING_POLL);
        if (useMemoryOnlySearch)
            modifiers = modifiers.add(ReadModifiers.MEMORY_ONLY_SEARCH);
        return gigaSpace.read(template, 0, modifiers);
    }

    @Override
    public String toString() {
        return "Single Exclusive Read, nonBlocking[" + nonBlocking + "], nonBlockingFactor[" + nonBlockingFactor
                + "], useFifoGroups[" + isUseFifoGrouping() + "], useMemoryOnlySearch[" + isUseMemoryOnlySearch() + "]";
    }
}
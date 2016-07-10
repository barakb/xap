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
 * Performs single take operation using {@link org.openspaces.core.GigaSpace#take(Object, long,
 * int)}.
 *
 * @author kimchy
 */
public class SingleTakeReceiveOperationHandler extends AbstractFifoGroupingReceiveOperationHandler {

    /**
     * Performs a single take operation using {@link org.openspaces.core.GigaSpace#take(Object,
     * long, int)} with the given timeout.
     */
    @Override
    protected Object doReceiveBlocking(Object template, GigaSpace gigaSpace, long receiveTimeout) throws DataAccessException {
        int modifiers = gigaSpace.getSpace().getReadModifiers();
        if (useFifoGrouping)
            modifiers |= TakeModifiers.FIFO_GROUPING_POLL;
        if (useMemoryOnlySearch)
            modifiers |= TakeModifiers.MEMORY_ONLY_SEARCH;
        return gigaSpace.take(template, receiveTimeout, modifiers);
    }

    /**
     * Performs a single take operation using {@link org.openspaces.core.GigaSpace#take(Object,
     * long, int)} with no timeout.
     */
    @Override
    protected Object doReceiveNonBlocking(Object template, GigaSpace gigaSpace) throws DataAccessException {
        int modifiers = gigaSpace.getSpace().getReadModifiers();
        if (useFifoGrouping)
            modifiers |= TakeModifiers.FIFO_GROUPING_POLL;
        if (useMemoryOnlySearch)
            modifiers |= TakeModifiers.MEMORY_ONLY_SEARCH;
        return gigaSpace.take(template, 0, modifiers);
    }

    @Override
    public String toString() {
        return "Single Take, nonBlocking[" + nonBlocking + "], nonBlockingFactor[" + nonBlockingFactor
                + "], useFifoGroups[" + isUseFifoGrouping() + "], useMemoryOnlySearch[" + isUseMemoryOnlySearch() + "]";
    }
}

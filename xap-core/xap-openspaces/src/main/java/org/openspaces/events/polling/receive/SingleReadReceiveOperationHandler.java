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
 * Performs single read operation using {@link org.openspaces.core.GigaSpace#read(Object, long)}.
 *
 * @author kimchy
 */
public class SingleReadReceiveOperationHandler extends AbstractMemoryOnlySearchReceiveOperationHandler {

    /**
     * Performs single read operation using {@link org.openspaces.core.GigaSpace#read(Object, long)}
     * with the given timeout.
     */
    @Override
    protected Object doReceiveBlocking(Object template, GigaSpace gigaSpace, long receiveTimeout) throws DataAccessException {
        int modifiers = gigaSpace.getSpace().getReadModifiers();
        if (useMemoryOnlySearch)
            modifiers |= ReadModifiers.MEMORY_ONLY_SEARCH;

        return gigaSpace.read(template, receiveTimeout, modifiers);
    }

    /**
     * Performs single read operation using {@link org.openspaces.core.GigaSpace#read(Object, long)}
     * with no timeout.
     */
    @Override
    protected Object doReceiveNonBlocking(Object template, GigaSpace gigaSpace) throws DataAccessException {
        int modifiers = gigaSpace.getSpace().getReadModifiers();
        if (useMemoryOnlySearch)
            modifiers |= ReadModifiers.MEMORY_ONLY_SEARCH;

        return gigaSpace.read(template, 0, modifiers);
    }

    @Override
    public String toString() {
        return "Single Read, nonBlocking[" + nonBlocking + "], nonBlockingFactor[" + nonBlockingFactor
                + "], useMemoryOnlySearch[" + isUseMemoryOnlySearch() + "]";
    }
}

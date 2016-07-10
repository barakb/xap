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

package com.gigaspaces.internal.server.space;

import com.gigaspaces.internal.utils.collections.ConcurrentHashSet;
import com.gigaspaces.internal.utils.collections.IAddOnlySet;
import com.j_spaces.core.OperationID;

import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author eitany
 * @since 9.0
 */
@com.gigaspaces.api.InternalApi
public class DuplicateOperationIDFilter implements IAddOnlySet<OperationID> {

    private final Set<OperationID> _operationIds = new ConcurrentHashSet<OperationID>();
    private final Queue<OperationID> _orderOfOperations = new ConcurrentLinkedQueue<OperationID>();
    private final AtomicInteger _size = new AtomicInteger(0);
    private final int _lastOperationCountToKeep;

    public DuplicateOperationIDFilter(int lastOperationCountToKeep) {
        _lastOperationCountToKeep = lastOperationCountToKeep;
    }

    /*
     * @see com.gigaspaces.internal.server.space.IAddOnlySet#contains(com.j_spaces.core.OperationID)
     */
    @Override
    public boolean contains(OperationID operationID) {
        if (operationID == null)
            return false;

        return _operationIds.contains(operationID);
    }

    /*
     * @see com.gigaspaces.internal.server.space.IAddOnlySet#add(com.j_spaces.core.OperationID)
     */
    @Override
    public void add(OperationID operationID) {
        if (operationID == null)
            return;
        _operationIds.add(operationID);
        _orderOfOperations.offer(operationID);
        if (_size.incrementAndGet() > _lastOperationCountToKeep) {
            OperationID poll = _orderOfOperations.poll();
            if (poll != null)
                _operationIds.remove(poll);
        }
    }

    @Override
    public void clear() {
        _operationIds.clear();
        _orderOfOperations.clear();
        _size.set(0);
    }

}

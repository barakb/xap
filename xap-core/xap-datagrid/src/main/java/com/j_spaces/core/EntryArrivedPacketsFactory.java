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

package com.j_spaces.core;

import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.j_spaces.core.server.processor.EntryArrivedPacket;
import com.j_spaces.kernel.pool.IResourceFactory;
import com.j_spaces.kernel.pool.ResourcePool;

import net.jini.core.transaction.server.ServerTransaction;

/**
 * a factory for entry-arrived packets, which holds an inner bound pool which fills on-demand.
 *
 * @author moran
 * @version 1.0
 * @since 5.1
 */
@com.gigaspaces.api.InternalApi
public class EntryArrivedPacketsFactory implements IResourceFactory<EntryArrivedPacket> {
    private final ResourcePool<EntryArrivedPacket> resourcePool = new ResourcePool<EntryArrivedPacket>(this /*factory*/, 0 /*min*/, 200 /*max*/);

    @Override
    public EntryArrivedPacket allocate() {
        return new EntryArrivedPacket();
    }

    /**
     * returns a constructed EntryArrivedPacket extracted from the pool of resources.
     *
     * @return a free resource which should be returned to the pool.
     */
    public EntryArrivedPacket getPacket(OperationID operationID, IEntryHolder entryHolder, ServerTransaction xtn,
                                        boolean notifyListeners, IEntryHolder entryValueToNotify, boolean fromReplication) {
        EntryArrivedPacket resource = resourcePool.getResource();
        resource.constructEntryArrivedPacket(operationID, entryHolder, xtn, notifyListeners,
                entryValueToNotify, fromReplication);
        return resource;
    }

    /**
     * Returns this packet to the pool as a free resource. Equivalent to calling {@link
     * com.j_spaces.kernel.pool.Resource#release()}
     *
     * @param resource the EntryArrivedPacket to be freed.
     */
    public void freePacket(EntryArrivedPacket resource) {
        resourcePool.freeResource(resource);
    }
}
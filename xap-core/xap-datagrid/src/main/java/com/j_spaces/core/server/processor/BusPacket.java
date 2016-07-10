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


package com.j_spaces.core.server.processor;

import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.gigaspaces.time.SystemTime;
import com.j_spaces.core.OperationID;
import com.j_spaces.kernel.pool.Resource;

import net.jini.core.transaction.server.ServerTransaction;

/**
 * A Bus Packet is an event that is sent on some MT-Queue in order to be handled by a Worker
 * Thread.
 */
public abstract class BusPacket<T> extends Resource {
    private IEntryHolder _entryHolder;
    private ServerTransaction _transaction;
    private long _timeout;
    private OperationID _operationID;

    // the time when the bus packet was created - used to indicate the end of the initial operation processing
    // by the engine
    private long _creationTime;

    /**
     * default constructor
     */
    protected BusPacket() {
        setCreationTime();
    }

    /**
     * Creates a new BusPacket object.
     */
    protected BusPacket(OperationID operationID, IEntryHolder entryHolder,
                        ServerTransaction xtn, long timeout) {
        constructBusPacket(operationID, entryHolder, xtn, timeout);
    }

    public abstract void execute(T executor) throws Exception;

    /**
     * Constructs a BusPacket.
     */
    public void constructBusPacket(OperationID operationID, IEntryHolder entryHolder, ServerTransaction xtn, long timeout) {
        _entryHolder = entryHolder;
        _transaction = xtn;
        _timeout = timeout;
        _operationID = operationID;

        setCreationTime();
    }

    private void setCreationTime() {
        _creationTime = SystemTime.timeMillis();
    }

    public OperationID getOperationID() {
        return _operationID;
    }

    protected void setOperationID(OperationID operationID) {
        this._operationID = operationID;
    }

    public long getCreationTime() {
        return _creationTime;
    }

    /*
     * @see com.j_spaces.kernel.pool.Resource#clear()
	 */
    @Override
    public void clear() {
        _entryHolder = null;
        _transaction = null;
        _timeout = 0;
        _operationID = null;
        _creationTime = 0;
    }

    public ServerTransaction getTransaction() {
        return _transaction;
    }

    public long getTimeout() {
        return _timeout;
    }

    public IEntryHolder getEntryHolder() {
        return _entryHolder;
    }
}


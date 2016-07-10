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
import com.j_spaces.core.OperationID;

import net.jini.core.transaction.server.ServerTransaction;

@com.gigaspaces.api.InternalApi
public class EntryArrivedPacket extends BusPacket<Processor> {
    /**
     * true if should notify listeners; false otherwise.
     */
    private boolean _shouldNotifyListeners;

    /**
     * contains original content of entry (before any updates).
     */
    private IEntryHolder _entryValueToNotify;

    private boolean _fromReplication;

    /**
     * Constructs an EntryArrivedPacket
     */
    public void constructEntryArrivedPacket(OperationID operationID, IEntryHolder entryHolder, ServerTransaction xtn,
                                            boolean notifyListeners, IEntryHolder entryValueToNotify, boolean fromReplication) {
        super.constructBusPacket(operationID, entryHolder, xtn, 0);
        _shouldNotifyListeners = notifyListeners;
        _entryValueToNotify = entryValueToNotify;
        _fromReplication = fromReplication;
    }

    /**
     * @see com.j_spaces.core.server.processor.BusPacket#clear()
     * @see com.j_spaces.kernel.pool.Resource#clear()
     */
    @Override
    public void clear() {
        //clear BusPacket
        super.clear();

        _shouldNotifyListeners = false;
        _entryValueToNotify = null;
        _fromReplication = false;
    }

    public boolean shouldNotifyListeners() {
        return _shouldNotifyListeners;
    }

    public IEntryHolder getEntryValueToNotify() {
        return _entryValueToNotify;
    }

    public boolean isFromReplication() {
        return _fromReplication;
    }

    @Override
    public void execute(Processor processor) throws Exception {
        processor.entryArrivedSA(this);
    }
}

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

/**
 * @author yael
 * @since 9.1
 */
@com.gigaspaces.api.InternalApi
public class EntryUpdatedPacket extends BusPacket<Processor> {

    /**
     * contains original entry and updated entry
     */
    final private IEntryHolder _originalEntryHolder;
    final private IEntryHolder _notifyEH;           //the image of entry after update op' will be used in "regular" notify
    final private boolean _fromReplication;
    final private boolean _notifyMatched;
    final private boolean _notifyRematched;


    /**
     * Constructs an EntryUpdatedPacket
     */
    public EntryUpdatedPacket(OperationID operationID, IEntryHolder originalEntryHolder, IEntryHolder newEntryHolder, IEntryHolder notifyEH,
                              ServerTransaction xtn, boolean fromReplication, boolean notifyMatch, boolean notifyRematch) {
        super(operationID, newEntryHolder, xtn, 0);

        _originalEntryHolder = originalEntryHolder;
        _notifyEH = notifyEH;
        _fromReplication = fromReplication;
        _notifyMatched = notifyMatch;
        _notifyRematched = notifyRematch;
    }

    @Override
    public void execute(Processor processor) throws Exception {
        processor.entryUpdatedSA(this);
    }

    public boolean isNotifyMatched() {
        return _notifyMatched;
    }

    public boolean isNotifyRematched() {
        return _notifyRematched;
    }

    public boolean isFromReplication() {
        return _fromReplication;
    }

    /**
     * @return the entry before the update operation
     */
    public IEntryHolder getOriginalEntryHolder() {
        return _originalEntryHolder;
    }

    public IEntryHolder getNotifyEH() {
        return _notifyEH;
    }
}

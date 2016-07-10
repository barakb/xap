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
 * A bus packet that is used to search for override notify templates.
 *
 * @author GuyK
 * @since 6.6
 */
@com.gigaspaces.api.InternalApi
public class EntryUnmatchedPacket extends BusPacket<Processor> {
    /**
     * contains original content of entry (before any updates).
     */
    final private IEntryHolder _newEntryHolder;

    final private boolean _fromReplication;


    /**
     * Constructs an EntryArrivedPacket
     */
    public EntryUnmatchedPacket(OperationID operationID, IEntryHolder originalEntryHolder, IEntryHolder newEntryHolder,
                                ServerTransaction xtn, boolean fromReplication) {
        super(operationID, originalEntryHolder, xtn, 0);

        _newEntryHolder = newEntryHolder;
        _fromReplication = fromReplication;
    }

    @Override
    public void execute(Processor processor) throws Exception {
        processor.entryUnmatchedSA(this);
    }

    /**
     * Indicates the source of the update operation that caused this search.
     */
    public boolean isFromReplication() {
        return _fromReplication;
    }

    public IEntryHolder getNewEntryHolder() {
        return _newEntryHolder;
    }
}

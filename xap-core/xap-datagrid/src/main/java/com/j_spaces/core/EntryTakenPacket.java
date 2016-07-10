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
import com.j_spaces.core.server.processor.BusPacket;
import com.j_spaces.core.server.processor.Processor;

import net.jini.core.transaction.server.ServerTransaction;


/*******************************************************************************
 * Copyright (c) 2010 GigaSpaces Technologies Ltd. All rights reserved
 *
 * The software source code is proprietary and confidential information of GigaSpaces. You may use
 * the software source code solely under the terms and limitations of The license agreement granted
 * to you by GigaSpaces.
 *******************************************************************************/

@com.gigaspaces.api.InternalApi
public class EntryTakenPacket extends BusPacket<Processor> {
    //contains original content of entry (before any updates)
    final public IEntryHolder m_EntryValueToNotify;
    final private boolean _fromReplication;

    public EntryTakenPacket(OperationID operationID, IEntryHolder entryHolder, ServerTransaction xtn,
                            IEntryHolder entryValueToNotify, boolean fromReplication) {
        super(operationID, entryHolder, xtn, 0);
        m_EntryValueToNotify = entryValueToNotify;
        _fromReplication = fromReplication;
    }

    public boolean isFromReplication() {
        return _fromReplication;
    }

    @Override
    public void execute(Processor processor) throws Exception {
        processor.handleEntryTakenSA(this);
    }
}
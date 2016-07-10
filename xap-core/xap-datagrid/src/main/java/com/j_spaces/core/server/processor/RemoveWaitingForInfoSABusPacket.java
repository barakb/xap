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
import com.gigaspaces.internal.server.storage.ITemplateHolder;
import com.j_spaces.core.OperationID;

/**
 * This bus packet is created for asynchronous handling of waiting for templates when an entry is
 * removed in <code>removeEntrySA()</code>.
 */
@com.gigaspaces.api.InternalApi
public class RemoveWaitingForInfoSABusPacket extends BusPacket<Processor> {
    // if m_Template is not null - remove only a specific template
    //the following 2 fields are not null only if op' requested
    //for a specific template
    private final ITemplateHolder _template;


    public RemoveWaitingForInfoSABusPacket(OperationID operationID, IEntryHolder entryHolder,
                                           ITemplateHolder template) {
        super(operationID, entryHolder, null, 0);
        _template = template;
    }


    @Override
    public void execute(Processor processor) throws Exception {
        processor.handleRemoveWaitingForInfoSA(this);
    }


    public ITemplateHolder getTemplate() {
        return _template;
    }


}
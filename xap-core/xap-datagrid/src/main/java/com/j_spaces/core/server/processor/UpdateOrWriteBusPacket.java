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

import com.gigaspaces.lrmi.nio.IResponseContext;
import com.gigaspaces.lrmi.nio.ReplyPacket;
import com.j_spaces.core.OperationID;
import com.j_spaces.core.UpdateOrWriteContext;

/**
 * a bus packet that will be processed by the processor and resubmit an update/write request to the
 * engine in a callback mode.
 *
 * @author asy ronen
 * @since 6.1
 */
@com.gigaspaces.api.InternalApi
public class UpdateOrWriteBusPacket extends BusPacket<Processor> {
    private final IResponseContext respContext;
    private final UpdateOrWriteContext ctx;
    private final ReplyPacket respPacket;

    public UpdateOrWriteBusPacket(OperationID operationID, IResponseContext respContext,
                                  UpdateOrWriteContext ctx) {
        this(operationID, respContext, ctx, null);
    }

    public UpdateOrWriteBusPacket(OperationID operationID, IResponseContext respContext,
                                  UpdateOrWriteContext ctx, ReplyPacket respPacket) {
        this.respContext = respContext;
        this.ctx = ctx;
        this.respPacket = respPacket;
        setOperationID(operationID);
    }

    @Override
    public void execute(Processor processor) throws Exception {
        processor.handleUpdateOrWrite(respContext, ctx, respPacket);
    }
}

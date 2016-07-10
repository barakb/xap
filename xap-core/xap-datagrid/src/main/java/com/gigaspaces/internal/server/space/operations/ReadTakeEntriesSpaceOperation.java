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

package com.gigaspaces.internal.server.space.operations;

import com.gigaspaces.internal.client.spaceproxy.operations.ReadTakeEntriesSpaceOperationRequest;
import com.gigaspaces.internal.client.spaceproxy.operations.ReadTakeEntriesSpaceOperationResult;
import com.gigaspaces.internal.server.space.SpaceImpl;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.lrmi.nio.IResponseContext;
import com.gigaspaces.lrmi.nio.ResponseContext;
import com.j_spaces.core.AnswerHolder;
import com.j_spaces.core.client.Modifiers;

/**
 * @author idan
 * @since 9.0
 */
@com.gigaspaces.api.InternalApi
public class ReadTakeEntriesSpaceOperation extends AbstractSpaceOperation<ReadTakeEntriesSpaceOperationResult, ReadTakeEntriesSpaceOperationRequest> {
    @Override
    public void execute(ReadTakeEntriesSpaceOperationRequest request, ReadTakeEntriesSpaceOperationResult result, SpaceImpl space, boolean oneway)
            throws Exception {
        IResponseContext respContext = ResponseContext.getResponseContext();
        if (respContext != null)
            respContext.setInvokedFromNewRouter(true);

        AnswerHolder answerHolder = space.readMultiple(request.getTemplatePacket(),
                request.getTransaction(), request.isTake(), request.getMaxResults(),
                request.getSpaceContext(), false, request.getModifiers(), request.getMinResultsToWaitFor(),
                request.getTimeOut(), request.isIfExist());

        final IEntryPacket[] entryPackets = answerHolder != null ? answerHolder.getEntryPackets() : null;
        result.setEntryPackets(entryPackets);
        if (answerHolder != null) {
            result.setSyncReplicationLevel(answerHolder.getSyncRelplicationLevel());
            if (Modifiers.contains(request.getModifiers(), Modifiers.LOG_SCANNED_ENTRIES_COUNT))
                result.setNumOfEntriesMatched(answerHolder.getNumOfEntriesMatched());
        }

    }

    @Override
    public String getLogName(ReadTakeEntriesSpaceOperationRequest request,
                             ReadTakeEntriesSpaceOperationResult result) {
        return request.isTake() ? "take entries" : "read entries";
    }
}

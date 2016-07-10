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

import com.gigaspaces.internal.client.spaceproxy.operations.ReadTakeEntriesByIdsSpaceOperationRequest;
import com.gigaspaces.internal.client.spaceproxy.operations.ReadTakeEntriesByIdsSpaceOperationResult;
import com.gigaspaces.internal.server.space.SpaceImpl;
import com.gigaspaces.internal.transport.IEntryPacket;

/**
 * @author idan
 * @since 9.0
 */
@com.gigaspaces.api.InternalApi
public class ReadTakeEntriesByIdsSpaceOperation extends AbstractSpaceOperation<ReadTakeEntriesByIdsSpaceOperationResult, ReadTakeEntriesByIdsSpaceOperationRequest> {
    @Override
    public void execute(ReadTakeEntriesByIdsSpaceOperationRequest request, ReadTakeEntriesByIdsSpaceOperationResult result, SpaceImpl space, boolean oneway)
            throws Exception {
        final IEntryPacket[] entries = space.readByIds(request.getTemplate(),
                request.getTransaction(),
                request.isTake(),
                request.getSpaceContext(),
                request.getModifiers());
        result.setEntryPackets(entries);
    }

    @Override
    public String getLogName(ReadTakeEntriesByIdsSpaceOperationRequest request,
                             ReadTakeEntriesByIdsSpaceOperationResult result) {
        return request.isTake() ? "take by ids" : "read by ids";
    }
}

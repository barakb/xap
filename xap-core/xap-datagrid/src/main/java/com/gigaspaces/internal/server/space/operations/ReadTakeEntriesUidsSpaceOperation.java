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

import com.gigaspaces.internal.client.spaceproxy.operations.ReadTakeEntriesUidsSpaceOperationRequest;
import com.gigaspaces.internal.client.spaceproxy.operations.ReadTakeEntriesUidsSpaceOperationResult;
import com.gigaspaces.internal.server.space.SpaceImpl;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.j_spaces.core.UidQueryPacket;

/**
 * @author idan
 * @since 9.0
 */
@com.gigaspaces.api.InternalApi
public class ReadTakeEntriesUidsSpaceOperation extends AbstractSpaceOperation<ReadTakeEntriesUidsSpaceOperationResult, ReadTakeEntriesUidsSpaceOperationRequest> {
    @Override
    public void execute(ReadTakeEntriesUidsSpaceOperationRequest request, ReadTakeEntriesUidsSpaceOperationResult result, SpaceImpl space, boolean oneway)
            throws Exception {
        final IEntryPacket[] entries = space.readMultiple(request.getTemplate(),
                request.getTransaction(),
                false,
                request.getEntriesLimit(),
                request.getSpaceContext(),
                true,
                request.getModifiers());
        final UidQueryPacket queryPacket = (UidQueryPacket) entries[0];
        result.setUids(queryPacket.getMultipleUIDs());
    }

    @Override
    public String getLogName(ReadTakeEntriesUidsSpaceOperationRequest request,
                             ReadTakeEntriesUidsSpaceOperationResult result) {
        return "read uids";
    }
}

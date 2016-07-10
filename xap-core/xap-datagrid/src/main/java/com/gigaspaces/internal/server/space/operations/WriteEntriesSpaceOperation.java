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

import com.gigaspaces.internal.client.spaceproxy.operations.WriteEntriesSpaceOperationRequest;
import com.gigaspaces.internal.client.spaceproxy.operations.WriteEntriesSpaceOperationResult;
import com.gigaspaces.internal.server.space.SpaceImpl;
import com.gigaspaces.lrmi.nio.IResponseContext;
import com.gigaspaces.lrmi.nio.ResponseContext;
import com.j_spaces.core.client.Modifiers;

/**
 * @author Niv Ingberg
 * @since 9.0.0
 */
@com.gigaspaces.api.InternalApi
public class WriteEntriesSpaceOperation extends AbstractSpaceOperation<WriteEntriesSpaceOperationResult, WriteEntriesSpaceOperationRequest> {
    @Override
    public void execute(WriteEntriesSpaceOperationRequest request, WriteEntriesSpaceOperationResult result, SpaceImpl space, boolean oneway)
            throws Exception {
        IResponseContext respContext = ResponseContext.getResponseContext();
        if (respContext != null)
            respContext.setInvokedFromNewRouter(true);

        int modifiers = request.getModifiers();
        modifiers = Modifiers.remove(modifiers, Modifiers.NO_RETURN_VALUE);
        modifiers = Modifiers.remove(modifiers, Modifiers.NO_WRITE_LEASE);
        WriteEntriesResult writeEntriesResult = space.write(request.getEntriesPackets(), request.getTransaction(),
                request.getLease(), request.getLeases(), request.getSpaceContext(), request.getTimeOut(), modifiers, true);

        if (writeEntriesResult != null) {
            for (int i = 0; i < writeEntriesResult.getSize(); i++)
                if (!writeEntriesResult.isError(i))
                    writeEntriesResult.getResults()[i].removeRedundantData(request.getEntriesPackets()[i].getTypeDescriptor(), modifiers);

            result.setResult(writeEntriesResult);
        }
    }

    @Override
    public String getLogName(WriteEntriesSpaceOperationRequest request,
                             WriteEntriesSpaceOperationResult result) {
        return "write entries";
    }
}

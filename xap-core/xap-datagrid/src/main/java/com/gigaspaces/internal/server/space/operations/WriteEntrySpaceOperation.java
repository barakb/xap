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

import com.gigaspaces.internal.client.spaceproxy.operations.WriteEntrySpaceOperationRequest;
import com.gigaspaces.internal.client.spaceproxy.operations.WriteEntrySpaceOperationResult;
import com.gigaspaces.internal.server.space.SpaceImpl;
import com.j_spaces.core.AnswerPacket;
import com.j_spaces.core.client.Modifiers;

/**
 * @author idan
 * @since 9.0.0
 */
@com.gigaspaces.api.InternalApi
public class WriteEntrySpaceOperation extends AbstractSpaceOperation<WriteEntrySpaceOperationResult, WriteEntrySpaceOperationRequest> {
    @Override
    public void execute(WriteEntrySpaceOperationRequest request, WriteEntrySpaceOperationResult result, SpaceImpl space, boolean oneway)
            throws Exception {
        WriteEntryResult writeEntryResult = null;

        int modifiers = request.getModifiers();
        modifiers = Modifiers.remove(modifiers, Modifiers.NO_RETURN_VALUE);
        modifiers = Modifiers.remove(modifiers, Modifiers.NO_WRITE_LEASE);

        if (request.isUpdate()) {
            AnswerPacket answerPacket = space.update(request.getEntryPacket(),
                    request.getTransaction(),
                    request.getLease(),
                    request.getTimeout(),
                    request.getSpaceContext(),
                    modifiers,
                    true /*newRouter*/);

            writeEntryResult = answerPacket.getWriteEntryResult();
        } else {
            writeEntryResult = space.write(request.getEntryPacket(),
                    request.getTransaction(),
                    request.getLease(),
                    modifiers,
                    false /* fromReplication*/,
                    request.getSpaceContext());
        }

        //Oneway has no result
        if (oneway)
            return;

        if (writeEntryResult != null) {
            writeEntryResult.removeRedundantData(request.getEntryPacket().getTypeDescriptor(), modifiers);
            result.setResult(writeEntryResult);
        }
    }

    @Override
    public String getLogName(WriteEntrySpaceOperationRequest request,
                             WriteEntrySpaceOperationResult result) {
        return "write";
    }
}

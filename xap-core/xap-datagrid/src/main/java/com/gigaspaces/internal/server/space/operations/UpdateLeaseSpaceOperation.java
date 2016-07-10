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

import com.gigaspaces.internal.client.spaceproxy.operations.UpdateLeaseSpaceOperationRequest;
import com.gigaspaces.internal.client.spaceproxy.operations.UpdateLeaseSpaceOperationResult;
import com.gigaspaces.internal.server.space.SpaceImpl;
import com.j_spaces.core.SpaceContext;

/**
 * @author Niv Ingberg
 * @since 9.0.0
 */
@com.gigaspaces.api.InternalApi
public class UpdateLeaseSpaceOperation extends AbstractSpaceOperation<UpdateLeaseSpaceOperationResult, UpdateLeaseSpaceOperationRequest> {
    @Override
    public void execute(UpdateLeaseSpaceOperationRequest request, UpdateLeaseSpaceOperationResult result, SpaceImpl space, boolean oneway)
            throws Exception {
        final SpaceContext spaceContext = request.getSpaceContext();
        final boolean isFromGateway = spaceContext != null ? spaceContext.isFromGateway() : false;
        if (request.isCancel())
            space.cancel(request.getUid(), request.getTypeName(), request.getLeaseObjectType(), isFromGateway);
        else
            space.renew(request.getUid(), request.getTypeName(), request.getLeaseObjectType(), request.getDuration(), isFromGateway);
    }

    @Override
    public String getLogName(UpdateLeaseSpaceOperationRequest request,
                             UpdateLeaseSpaceOperationResult result) {
        return "update lease";
    }
}

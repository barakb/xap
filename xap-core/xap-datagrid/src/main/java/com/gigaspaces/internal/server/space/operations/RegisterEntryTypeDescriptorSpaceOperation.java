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

import com.gigaspaces.internal.client.spaceproxy.operations.RegisterEntryTypeDescriptorSpaceOperationRequest;
import com.gigaspaces.internal.client.spaceproxy.operations.RegisterEntryTypeDescriptorSpaceOperationResult;
import com.gigaspaces.internal.server.space.SpaceImpl;
import com.gigaspaces.security.authorities.SpaceAuthority.SpacePrivilege;

/**
 * @author Niv Ingberg
 * @since 9.0.0
 */
@com.gigaspaces.api.InternalApi
public class RegisterEntryTypeDescriptorSpaceOperation extends AbstractSpaceOperation<RegisterEntryTypeDescriptorSpaceOperationResult, RegisterEntryTypeDescriptorSpaceOperationRequest> {
    @Override
    public void execute(RegisterEntryTypeDescriptorSpaceOperationRequest request, RegisterEntryTypeDescriptorSpaceOperationResult result, SpaceImpl space, boolean oneway)
            throws Exception {
        space.beforeTypeOperation(true, request.getSpaceContext(), SpacePrivilege.ALTER, request.getTypeDesc().getTypeName());
        space.getEngine().getMemoryManager().monitorMemoryUsage(true);

        // Register type descriptor:
        space.getEngine().registerTypeDesc(request.getTypeDesc(), request.isGatewayProxy());
    }

    @Override
    public String getLogName(
            RegisterEntryTypeDescriptorSpaceOperationRequest request,
            RegisterEntryTypeDescriptorSpaceOperationResult result) {
        return "register type descriptor";
    }
}

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

import com.gigaspaces.events.GSEventRegistration;
import com.gigaspaces.internal.client.spaceproxy.operations.RegisterEntriesListenerSpaceOperationRequest;
import com.gigaspaces.internal.client.spaceproxy.operations.RegisterEntriesListenerSpaceOperationResult;
import com.gigaspaces.internal.server.space.SpaceImpl;

/**
 * Executes notify operation on the space
 *
 * @author anna
 * @since 9.0.0
 */
@com.gigaspaces.api.InternalApi
public class RegisterEntriesListenerSpaceOperation extends AbstractSpaceOperation<RegisterEntriesListenerSpaceOperationResult, RegisterEntriesListenerSpaceOperationRequest> {
    @Override
    public void execute(RegisterEntriesListenerSpaceOperationRequest request, RegisterEntriesListenerSpaceOperationResult result, SpaceImpl space, boolean oneway)
            throws Exception {
        GSEventRegistration eventRegistration = space.notify(request.getTemplatePacket(), null, request.getLease(), request.getSpaceContext(), request.getNotifyInfo());
        result.setEventRegistration(eventRegistration);
    }

    @Override
    public String getLogName(
            RegisterEntriesListenerSpaceOperationRequest request,
            RegisterEntriesListenerSpaceOperationResult result) {
        return "register listener";
    }
}

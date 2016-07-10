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

import com.gigaspaces.internal.client.spaceproxy.operations.PrepareAndCommitTransactionSpaceOperationRequest;
import com.gigaspaces.internal.client.spaceproxy.operations.PrepareAndCommitTransactionSpaceOperationResult;
import com.gigaspaces.internal.server.space.SpaceImpl;

/**
 * @author eitany
 * @since 9.0
 */
@com.gigaspaces.api.InternalApi
public class PrepareAndCommitTransactionSpaceOperation extends AbstractSpaceOperation<PrepareAndCommitTransactionSpaceOperationResult, PrepareAndCommitTransactionSpaceOperationRequest> {
    @Override
    public void execute(PrepareAndCommitTransactionSpaceOperationRequest request, PrepareAndCommitTransactionSpaceOperationResult result, SpaceImpl space, boolean oneway)
            throws Exception {
        int state;
        if (request.getXid() == null)
            state = space.prepareAndCommitImpl(request.getMgr(), request.getId(), request.getOperationID());
        else
            state = space.prepareAndCommitImpl(request.getMgr(), request.getXid(), request.getOperationID());
        result.setState(state);
    }

    @Override
    public boolean isGenericLogging() {
        return false;
    }

    @Override
    public String getLogName(
            PrepareAndCommitTransactionSpaceOperationRequest request,
            PrepareAndCommitTransactionSpaceOperationResult result) {
        return "prepare and commit";
    }

}

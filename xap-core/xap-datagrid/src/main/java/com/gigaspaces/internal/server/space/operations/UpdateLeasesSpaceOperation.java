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

import com.gigaspaces.internal.client.spaceproxy.operations.UpdateLeasesSpaceOperationRequest;
import com.gigaspaces.internal.client.spaceproxy.operations.UpdateLeasesSpaceOperationResult;
import com.gigaspaces.internal.server.space.SpaceImpl;
import com.j_spaces.core.LeaseManager;

/**
 * @author Niv Ingberg
 * @since 9.0.0
 */
@com.gigaspaces.api.InternalApi
public class UpdateLeasesSpaceOperation extends AbstractSpaceOperation<UpdateLeasesSpaceOperationResult, UpdateLeasesSpaceOperationRequest> {
    @Override
    public void execute(UpdateLeasesSpaceOperationRequest request, UpdateLeasesSpaceOperationResult result, SpaceImpl space, boolean oneway)
            throws Exception {
        space.beforeOperation(true, false /*checkQuiesceMode*/, null);

        Exception[] errors = null;
        final LeaseManager leaseManager = space.getEngine().getLeaseManager();
        for (int i = 0; i < request.getSize(); i++) {
            try {
                leaseManager.update(request.getUid(i), request.getTypeName(i), request.getLeaseObjectType(i),
                        request.getDurations(i));
            } catch (Exception e) {
                if (errors == null)
                    errors = new Exception[request.getSize()];
                errors[i] = e;
            }
        }

        result.setErrors(errors);
    }

    @Override
    public String getLogName(UpdateLeasesSpaceOperationRequest request,
                             UpdateLeasesSpaceOperationResult result) {
        return "update leases";
    }
}

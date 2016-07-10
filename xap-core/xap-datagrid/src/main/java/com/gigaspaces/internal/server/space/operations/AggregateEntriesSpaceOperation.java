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

import com.gigaspaces.internal.client.spaceproxy.operations.AggregateEntriesSpaceOperationRequest;
import com.gigaspaces.internal.client.spaceproxy.operations.AggregateEntriesSpaceOperationResult;
import com.gigaspaces.internal.server.space.SpaceImpl;
import com.gigaspaces.query.aggregators.AggregationInternalUtils;
import com.gigaspaces.security.authorities.SpaceAuthority;

/**
 * @author Niv Ingberg
 * @since 10.0
 */
@com.gigaspaces.api.InternalApi
public class AggregateEntriesSpaceOperation extends AbstractSpaceOperation<AggregateEntriesSpaceOperationResult, AggregateEntriesSpaceOperationRequest> {

    @Override
    public void execute(AggregateEntriesSpaceOperationRequest request, AggregateEntriesSpaceOperationResult result, SpaceImpl space, boolean oneway)
            throws Exception {

        SpaceAuthority.SpacePrivilege requiredPrivilege = AggregationInternalUtils.containsCustomAggregators(request.getAggregators())
                ? SpaceAuthority.SpacePrivilege.EXECUTE
                : SpaceAuthority.SpacePrivilege.READ;

        space.beginPacketOperation(true, request.getSpaceContext(), requiredPrivilege, request.getQueryPacket());

        space.getEngine().aggregate(request.getQueryPacket(), request.getAggregators(), request.getReadModifiers(), request.getSpaceContext());

        Object[] intermediateResults = new Object[request.getAggregators().size()];
        for (int i = 0; i < intermediateResults.length; i++)
            intermediateResults[i] = request.getAggregators().get(i).getIntermediateResult();
        result.setIntermediateResults(intermediateResults);
    }

    @Override
    public String getLogName(AggregateEntriesSpaceOperationRequest request, AggregateEntriesSpaceOperationResult result) {
        return "scan";
    }
}

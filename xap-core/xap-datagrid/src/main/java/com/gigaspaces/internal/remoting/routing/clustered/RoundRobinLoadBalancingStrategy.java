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

package com.gigaspaces.internal.remoting.routing.clustered;

import com.gigaspaces.internal.remoting.RemoteOperationRequest;
import com.gigaspaces.internal.utils.concurrent.CyclicAtomicInteger;
import com.gigaspaces.internal.utils.concurrent.VolatileArray;

import java.util.Arrays;
import java.util.Collection;

/**
 * @author Niv Ingberg
 * @since 9.1.2
 */
@com.gigaspaces.api.InternalApi
public class RoundRobinLoadBalancingStrategy extends SpaceProxyLoadBalancingStrategy {
    private final String[] _membersNames;
    private final VolatileArray<RemoteOperationsExecutorProxy> _members;
    private final CyclicAtomicInteger _defaultLoadBalancer;
    private final CyclicAtomicInteger[] _operationsLoadBalancers;

    public RoundRobinLoadBalancingStrategy(RemoteOperationsExecutorsCluster cluster, Collection<String> membersNames, int numOfOperationsTypes) {
        super(cluster);

        final int size = membersNames.size();
        this._membersNames = membersNames.toArray(new String[size]);
        Arrays.sort(_membersNames);
        this._members = new VolatileArray<RemoteOperationsExecutorProxy>(size);
        this._defaultLoadBalancer = new CyclicAtomicInteger(size - 1);
        this._operationsLoadBalancers = new CyclicAtomicInteger[numOfOperationsTypes];
        for (int i = 0; i < _operationsLoadBalancers.length; i++)
            _operationsLoadBalancers[i] = new CyclicAtomicInteger(size - 1);
    }

    @Override
    public RemoteOperationsExecutorProxy getCandidate(RemoteOperationRequest<?> request) {
        CyclicAtomicInteger loadBalancer = request == null ? _defaultLoadBalancer : _operationsLoadBalancers[request.getOperationCode()];
        final int memberId = loadBalancer.getAndIncrement();
        RemoteOperationsExecutorProxy candidate = _members.get(memberId);
        if (candidate != null)
            return candidate;
        // If the candidate is disconnected, scan for a connected candidate:
        final int numOfMembers = _membersNames.length;
        for (int offset = 1; offset < numOfMembers; offset++) {
            int index = memberId + offset;
            if (index >= numOfMembers)
                index -= numOfMembers;
            candidate = _members.get(index);
            if (candidate != null)
                return candidate;
        }
        // If there are no connected candidates:
        return null;
    }

    @Override
    public void onMemberConnected(RemoteOperationsExecutorProxy connectedMember) {
        _members.set(indexOf(connectedMember.getName()), connectedMember);
    }

    @Override
    public void onMemberDisconnected(String disconnectedMemberName) {
        _members.set(indexOf(disconnectedMemberName), null);
    }

    private int indexOf(String memberName) {
        return Arrays.binarySearch(_membersNames, memberName);
    }

    @Override
    protected void updateActiveProxy(RemoteOperationsExecutorProxy newActiveProxy) {
        if (newActiveProxy != null) {
            onMemberConnected(newActiveProxy);
        }
    }
}

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

package com.gigaspaces.internal.server.space.recovery.group;

import com.gigaspaces.internal.cluster.node.replica.ISpaceSynchronizeReplicaState;
import com.gigaspaces.internal.server.space.SpaceImpl;
import com.j_spaces.core.client.SpaceURL;
import com.j_spaces.core.cluster.ClusterPolicy;

import java.util.LinkedList;
import java.util.List;

/**
 * ReplicationGroupRecovery defines how to recover in replication group
 *
 * @author anna
 * @since 6.0
 */
@com.gigaspaces.api.InternalApi
public class ReplicationGroupRecovery extends RecoveryGroup {


    /**
     * @param space
     */
    public ReplicationGroupRecovery(SpaceImpl space) {
        super(space);

    }

    /**
     * 1. try to recover from some primary space in replication group. 2. try to recover from sync
     * target in replication group. 3. try to recover from any space in replication group.
     */
    public ISpaceSynchronizeReplicaState recover(boolean transientOnly, boolean memoryOnly) {
        // Exclude the failover members , since they are used separately in failover recovery
        List<String> excludeTargets = getExcludeTargets();

        List<SpaceURL> recTargets = _space.getClusterPolicy().m_ReplicationPolicy.getRecoverableTargets(excludeTargets);

        return recover(recTargets, transientOnly, memoryOnly);
    }

    public List<String> getExcludeTargets() {
        List<String> excludeTargets = new LinkedList<String>();

        // add the mirror to the exclude list
        ClusterPolicy clusterPolicy = _space.getClusterPolicy();
        if (clusterPolicy.getReplicationPolicy().getMirrorServiceConfig() != null)
            excludeTargets.add(clusterPolicy.getReplicationPolicy().getMirrorServiceConfig().memberName);

        if (clusterPolicy.m_FailOverPolicy != null)
            excludeTargets.addAll(clusterPolicy.m_FailOverPolicy.getElectionGroupMembers());
        return excludeTargets;
    }
}
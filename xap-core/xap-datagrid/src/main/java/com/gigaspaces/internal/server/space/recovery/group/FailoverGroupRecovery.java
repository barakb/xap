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

import java.util.List;

/**
 * FailoverGroupRecovery defines how to recover in failover group
 *
 * @author anna
 * @since 6.0
 */
@com.gigaspaces.api.InternalApi
public class FailoverGroupRecovery extends RecoveryGroup {


    /**
     * @param space
     */
    public FailoverGroupRecovery(SpaceImpl space) {
        super(space);

    }

    @Override
    public ISpaceSynchronizeReplicaState recover(boolean transientOnly, boolean memoryOnly) {
        // recover from primary targets in fail over group
        List<SpaceURL> recTargets = _space.getClusterPolicy().m_FailOverPolicy.getRecoverableTargets(_space.getClusterPolicy().m_ClusterGroupMember);

        return recover(recTargets, transientOnly, memoryOnly);
    }
}
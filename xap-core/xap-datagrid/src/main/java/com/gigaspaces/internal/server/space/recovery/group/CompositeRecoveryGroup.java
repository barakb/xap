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

/**
 *
 */
package com.gigaspaces.internal.server.space.recovery.group;

import com.gigaspaces.internal.cluster.node.replica.ISpaceSynchronizeReplicaState;
import com.gigaspaces.internal.server.space.SpaceImpl;
import com.j_spaces.core.client.FinderException;

import java.rmi.RemoteException;
import java.util.LinkedList;

/**
 * Recover from one of the given recovery groups.
 *
 * @author anna
 * @since 7.0.1
 */
@com.gigaspaces.api.InternalApi
public class CompositeRecoveryGroup
        extends RecoveryGroup {

    private final LinkedList<RecoveryGroup> _recoveryGroups = new LinkedList<RecoveryGroup>();


    /**
     * @param space
     */
    public CompositeRecoveryGroup(SpaceImpl space) {
        super(space);

    }

    /* (non-Javadoc)
     * @see com.gigaspaces.internal.server.space.recovery.RecoveryGroup#recover(boolean)
     */
    @Override
    public ISpaceSynchronizeReplicaState recover(boolean transientOnly, boolean memoryOnly) throws Exception {
        for (RecoveryGroup recoveryGroup : _recoveryGroups) {
            ISpaceSynchronizeReplicaState recoveryStatus = recoveryGroup.recover(transientOnly, memoryOnly);

            //if no target was found - continue to the next
            if (recoveryStatus == null)
                continue;

            // If recovery succeeded - finished
            if (recoveryStatus.getCopyResult().isSuccessful())
                return recoveryStatus;

            // if recovery failed to recover - stop recovery
            // if failed because no recovery member was found - try another member
            if (!(recoveryStatus.getCopyResult().getFailureReason() instanceof FinderException || recoveryStatus.getCopyResult()
                    .getFailureReason() instanceof RemoteException))
                throw recoveryStatus.getCopyResult().getFailureReason();
        }

        return null;
    }

    /**
     * @param group
     */
    public void add(RecoveryGroup group) {
        _recoveryGroups.add(group);
    }

}

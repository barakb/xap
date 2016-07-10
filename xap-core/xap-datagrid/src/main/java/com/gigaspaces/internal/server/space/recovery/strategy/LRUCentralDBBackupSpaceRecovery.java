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
package com.gigaspaces.internal.server.space.recovery.strategy;

import com.gigaspaces.internal.cluster.node.replica.ISpaceSynchronizeReplicaState;
import com.gigaspaces.internal.server.space.SpaceImpl;

@com.gigaspaces.api.InternalApi
public class LRUCentralDBBackupSpaceRecovery extends BackupSpaceRecovery {
    /**
     * @param space
     */
    public LRUCentralDBBackupSpaceRecovery(SpaceImpl space) {
        super(space);
    }

    @Override
    public ISpaceSynchronizeReplicaState recoverFromOtherSpace() throws Exception {
        if (_space.getEngine().hasMirror())
            return recoverFromPrimaryMemoryOnly();

        return recoverTransientFromPrimary();

    }

    /* (non-Javadoc)
     * @see com.j_spaces.core.RecoveryManager.BackupSpaceRecovery#recoverFromDB()
     */
    @Override
    public void recoverFromDB() throws Exception {
        _space.initAndRecoverFromDataStorage(false);
    }
}
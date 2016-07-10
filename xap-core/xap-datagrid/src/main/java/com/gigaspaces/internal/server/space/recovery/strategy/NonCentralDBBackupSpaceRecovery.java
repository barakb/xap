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
public class NonCentralDBBackupSpaceRecovery extends BackupSpaceRecovery {
    /**
     * @param space
     */
    public NonCentralDBBackupSpaceRecovery(SpaceImpl space) {
        super(space);
    }

    @Override
    public ISpaceSynchronizeReplicaState recoverFromOtherSpace() throws Exception {
        // check if all the entries should be recovered,or only the transient ones 
        // if the engine didn't load any data - recover everything from another space
        // if space was started and data was loaded from database,
        // only transient entries should be copied from the target space because all persistent entries
        // were retrieved from the DB.
        boolean transientOnly = !_space.getEngine().isColdStart();

        return recoverFromPrimary(transientOnly, transientOnly);
    }

    /* (non-Javadoc)
     * @see com.j_spaces.core.RecoveryManager.BackupSpaceRecovery#recoverFromDB()
     */
    @Override
    public void recoverFromDB() throws Exception {
        _space.initAndRecoverFromDataStorage(true);
    }
}
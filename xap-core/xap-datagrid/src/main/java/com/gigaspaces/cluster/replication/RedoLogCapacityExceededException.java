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

package com.gigaspaces.cluster.replication;

import com.gigaspaces.client.ResourceCapacityExceededException;

/**
 * Thrown when the redo log reach its planned capacity and operations should be blocked until the
 * redolog size is reduced
 *
 * @author eitany
 * @since 7.1
 */
@com.gigaspaces.api.InternalApi
public class RedoLogCapacityExceededException
        extends ResourceCapacityExceededException {
    private final String targetName;
    private final String spaceName;

    public RedoLogCapacityExceededException(long redoLogSize, long maxBacklogAllowed, String targetName, String spaceName) {
        super("This operation cannot be performed because it needs to be replicated and the current replication backlog capacity reached " +
                "[" + redoLogSize + "/" + maxBacklogAllowed + "], backlog is kept for replication channel from " + spaceName + " to target " + targetName +
                ". Retry the operation once the backlog size is reduced");
        this.targetName = targetName;
        this.spaceName = spaceName;
    }

    //Used by new replication module
    public RedoLogCapacityExceededException(String message, String targetName, String spaceName) {
        super(message);
        this.targetName = targetName;
        this.spaceName = spaceName;
    }

    /**
     * @return name of the target that the redo log is kept for
     */
    public String getTargetName() {
        return targetName;
    }

    /**
     * @return name of the space that is keeping the redo log
     */
    public String getSpaceName() {
        return spaceName;
    }

    /** */
    private static final long serialVersionUID = 1L;

}

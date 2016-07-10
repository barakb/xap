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

package com.gigaspaces.internal.cluster.node.impl.replica;

import com.j_spaces.core.cluster.IReplicationFilterEntry;


public interface ISingleStageReplicaDataProducer<T extends ISpaceReplicaData> {

    public enum CloseStatus {
        CLOSED, CLOSING;
    }

    public static final long FORCED_CLOSE_WAIT_TIME = 500;

    /**
     * Closes the producer, frees up resources held in order to service the replica request.
     *
     * @return CloseStatus - the status of the close operation, can be CLOSED and CLOSING (another
     * thread is closing already)
     */
    public CloseStatus close(boolean forced);

    /**
     * Produce next data needed for replica, this call will always be followed by {@link
     * #releaseLockedData(ISpaceReplicaData)} if lockedData is true, it should keep that item
     * represented by this data locked until the release is called This is done in order to ensure
     * synchronization correctness
     *
     * @param synchCallback callback to the synchronization processor that needs to be called safely
     *                      under lock
     * @return next data needed for replica, null if done. Once null is returns all executive calls
     * must return null!
     */
    public T produceNextData(ISynchronizationCallback synchCallback);

    public IReplicationFilterEntry toFilterEntry(T data);

    public String dumpState();

    public String getName();

}
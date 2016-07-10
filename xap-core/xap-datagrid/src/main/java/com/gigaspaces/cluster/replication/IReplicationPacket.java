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
package com.gigaspaces.cluster.replication;

import com.j_spaces.core.cluster.ReplicationOperationType;


/**
 * Interface for replication packets - SyncPacket and CompositeReplicationPacket
 *
 * @author anna
 * @since 6.5
 */
public interface IReplicationPacket
        extends Cloneable

{

    public void setKey(long key);

    public long getKey();

    public boolean isTransient();

    public void discard();

    public boolean isDiscarded();

    public String getUid();

    public IReplicationPacket clone();

    /**
     * @return the operation
     */
    public ReplicationOperationType getOperation();

}
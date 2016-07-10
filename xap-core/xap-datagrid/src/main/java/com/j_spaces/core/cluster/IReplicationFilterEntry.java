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


package com.j_spaces.core.cluster;

import com.j_spaces.core.filters.entry.IFilterEntry;

/**
 * Represents an entry instance passed to the {@link IReplicationFilter}.
 *
 * @author Guy Korland
 * @see IReplicationFilter
 * @since 5.0
 */
public interface IReplicationFilterEntry extends IFilterEntry {

    /**
     * Gets the code of the replicated operation.
     *
     * @return the operation code.
     */
    ReplicationOperationType getOperationType();

    /**
     * Gets the object type as defined at {@link com.j_spaces.core.ObjectTypes ObjectTypes}.
     *
     * @return the object type
     * @see com.j_spaces.core.ObjectTypes
     */
    int getObjectType();

    /**
     * Sets the object values array.
     *
     * @param values the new values array
     */
    void setFieldsValues(Object[] values);

    /**
     * Sets the lease time.
     *
     * @param time time to live.
     * @see net.jini.core.lease.Lease
     */
    void setTimeToLive(long time);

    /**
     * Discard this entry from the replication.
     */
    void discard();

    /**
     * Checks if this entry was discarded.
     *
     * @return <code>true</code> if this entry was discarded
     */
    boolean isDiscarded();
}

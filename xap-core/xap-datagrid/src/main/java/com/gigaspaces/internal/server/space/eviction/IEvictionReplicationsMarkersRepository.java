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

package com.gigaspaces.internal.server.space.eviction;

import com.gigaspaces.internal.cluster.node.impl.backlog.sync.IMarker;

/**
 * repository for replication markers - used in order to prevent eviction of entries that did not
 * reach the async replication target yet (currently mirror)
 *
 * @author Yechiel Feffer
 * @since 9.1
 */
public interface IEvictionReplicationsMarkersRepository {
    /*
     * insert a marker for the given uid
     */
    void insert(String uid, IMarker marker, boolean alreadyLocked);

    /*
     * can the specified entry represented by the uid be evicted? i.e. is there
     * a valid unreached marker ? alreadyLocked : true if the repository is
     * already locked by the caller (using the getLockObject & releaseLockObject
     * methods)
     */
    boolean isEntryEvictable(String uid, boolean alreadyLocked);

    /*
     * get an object that when locked (via synchronized) will lock the
     * appropriate repository section related to the uid
     */
    Object getLockObject(String uid);

    void releaseLockObject(Object lockObject);

    /**
     * Gets the marker associated to this uid, null of none exists.
     */
    IMarker getMarker(String uid);

    /**
     * reap markers reached
     *
     * @return # of reaped markers
     */
    int reapUnused();

    /**
     * # of uids inside.
     */
    int size();
}

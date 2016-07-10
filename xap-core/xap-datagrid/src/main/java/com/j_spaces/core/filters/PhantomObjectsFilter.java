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

package com.j_spaces.core.filters;

import com.gigaspaces.internal.utils.collections.ConcurrentHashSet;
import com.gigaspaces.time.SystemTime;
import com.j_spaces.core.IJSpace;
import com.j_spaces.core.cluster.IReplicationFilter;
import com.j_spaces.core.cluster.IReplicationFilterEntry;
import com.j_spaces.core.cluster.ReplicationOperationType;
import com.j_spaces.core.cluster.ReplicationPolicy;

/**
 * Filter that blocks phantom objects (already been removed) to reinsert by the replication.
 *
 * @author Guy korland
 * @version 6.6.4
 */
@com.gigaspaces.api.InternalApi
public class PhantomObjectsFilter implements IReplicationFilter {

    final private PhantomTable phantomTable = new PhantomTable();

    /**
     * A set that holds uids for a minimum of a pre-set time. The time can be set using
     * com.gs.replication.phantom.interval property.
     *
     * @author GuyK
     */
    private static class PhantomTable {
        final static private long INTERVAL = Long.getLong("com.gs.replication.phantom.interval", 60 * 1000); // 1 min

        final private ConcurrentHashSet<Object> _idSet0 = new ConcurrentHashSet<Object>();
        final private ConcurrentHashSet<Object> _idSet1 = new ConcurrentHashSet<Object>();

        volatile private long lastClean = SystemTime.timeMillis();
        volatile private boolean currentSet;

        /**
         * Adds a new UID
         */
        public void add(String uid) {
            cleanIfNeeded();
            if (currentSet) {
                _idSet0.add(uid);
            } else {
                _idSet1.add(uid);
            }
        }

        /**
         * ] Checks if the set contains this uid.
         */
        public boolean contains(String uid) {
            cleanIfNeeded();
            return _idSet0.contains(uid) || _idSet1.contains(uid);
        }

        private void cleanIfNeeded() {
            long currentTimeMillis = SystemTime.timeMillis();
            if (lastClean + INTERVAL < currentTimeMillis) {
                synchronized (this) {
                    currentTimeMillis = SystemTime.timeMillis();
                    if (lastClean + INTERVAL < currentTimeMillis) {
                        if (currentSet) { // if current _idSet0 clean the other map
                            _idSet1.clear();
                            currentSet = false;
                        } else {
                            _idSet0.clear();
                            currentSet = true;
                        }
                        lastClean = currentTimeMillis;
                    }
                }
            }
        }
    }

    /**
     * Checks if the UID provided by read in the phantomTable then block it. If a Taks is called
     * adds the UID to the set.
     */
    public void process(int direction, IReplicationFilterEntry replicationFilterEntry,
                        String remoteSpaceMemberName) {
        ReplicationOperationType operationType = replicationFilterEntry.getOperationType();

        String uid = replicationFilterEntry.getUID();

        switch (operationType) {
            case TAKE:
                if (uid != null) {
                    phantomTable.add(uid);
                }
                break;

            case WRITE:
            case UPDATE:
                if (phantomTable.contains(uid)) {
                    replicationFilterEntry.discard();
                }
                break;
        }

    }

    public void close() throws RuntimeException {
    }

    public void init(IJSpace space, String paramUrl, ReplicationPolicy replicationPolicy) {
    }

}
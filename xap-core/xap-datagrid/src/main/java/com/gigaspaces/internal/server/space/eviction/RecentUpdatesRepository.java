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

import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.gigaspaces.time.SystemTime;

import net.jini.core.transaction.server.ServerTransaction;

import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

/**
 * keep info regarding recentUpdates- relevant for evictable entries
 *
 * @author yechielf
 */
@com.gigaspaces.api.InternalApi
public class RecentUpdatesRepository {
    //for persistent only - contains UIDs for entries recently updated from DB
    final private ConcurrentHashMap<String, RecentUpdateInfo> _recentUpdates;

    public RecentUpdatesRepository() {
        _recentUpdates = new ConcurrentHashMap<String, RecentUpdateInfo>();
    }


    public void insertToRecentUpdates(IEntryHolder entry, long duration, ServerTransaction xtn) {
        _recentUpdates.put(entry.getUID(), new RecentUpdateInfo(entry.getUID(), duration > 0 ? duration : SystemTime.timeMillis(), xtn));
    }

    public boolean removeFromRecentUpdates(IEntryHolder entry) {
        return (_recentUpdates.remove(entry.getUID()) != null);
    }

    public RecentUpdateInfo getRecentUpdateInfo(String uid) {
        return _recentUpdates.get(uid);
    }

    public Iterator<RecentUpdateInfo> getRecentUpdatesIterator() {
        return _recentUpdates.values().iterator();
    }

    public int size() {
        return _recentUpdates.size();
    }

    public final static class RecentUpdateInfo {
        private final String _uid;
        private final Long _timeBase;   //from it LeaseManager determines  to clear the recent-updated info
        private final ServerTransaction _xtn;  //not null if this info is inserted in commit of xtn

        public RecentUpdateInfo(String uid, long timeBase, ServerTransaction xtn) {
            _uid = uid;
            _timeBase = timeBase;
            _xtn = xtn;
        }

        public String getUid() {
            return _uid;
        }

        public long getTimeBase() {
            return _timeBase;
        }

        public ServerTransaction getXtn() {
            return _xtn;
        }

    }

}

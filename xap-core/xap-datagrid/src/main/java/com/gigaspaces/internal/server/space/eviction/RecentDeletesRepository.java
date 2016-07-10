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
 * keep info regarding recentDeletes- relevant for evictable entries
 *
 * @author yechielf
 */
@com.gigaspaces.api.InternalApi
public class RecentDeletesRepository {
    //for persistent only - contains UIDs for entries recently deleted from DB
    final private ConcurrentHashMap<String, RecentDeleteInfo> _recentDeletes;

    public RecentDeletesRepository() {
        _recentDeletes = new ConcurrentHashMap<String, RecentDeleteInfo>();
    }


    public void insertToRecentDeletes(IEntryHolder entry, long duration, ServerTransaction xtn) {
        _recentDeletes.put(entry.getUID(), new RecentDeleteInfo(entry.getUID(), duration > 0 ? duration : SystemTime.timeMillis(), xtn));
    }

    public boolean removeFromRecentDeletes(IEntryHolder entry) {
        return (_recentDeletes.remove(entry.getUID()) != null);
    }

    public RecentDeleteInfo getRecentDeleteInfo(String uid) {
        return _recentDeletes.get(uid);
    }

    public Iterator<RecentDeleteInfo> getRecentDeletesIterator() {
        return _recentDeletes.values().iterator();
    }

    public int size() {
        return _recentDeletes.size();
    }

    public final static class RecentDeleteInfo {
        private final String _uid;
        private final Long _timeBase;   //from it LeaseManager determines  to clear the recent-deleted entry & this info
        private final ServerTransaction _xtn;  //not null if this info is inserted in commit of xtn

        public RecentDeleteInfo(String uid, long timeBase, ServerTransaction xtn) {
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

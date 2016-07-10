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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * repository for replication markers - used in order to prevent eviction of entries that did not
 * reach the async replication target yet (currently mirror)
 *
 * @author Yechiel Feffer
 * @since 9.1
 */
@com.gigaspaces.api.InternalApi
public class EvictionReplicationsMarkersRepository implements IEvictionReplicationsMarkersRepository {
    public static final int NUM_OF_IMARKERS_REPOSITORY_SEGMENTS = 2000;

    private final EvictionReplicationsMarkersSegment[] _segments;
    private final AtomicInteger _size;

    public static final class EvictionReplicationsMarkersSegment {
        private final HashMap<String, IMarker> _uidsMap;
        private final Object _lockObject;

        EvictionReplicationsMarkersSegment() {
            _uidsMap = new HashMap<String, IMarker>();
            _lockObject = new Object();
        }

        Object getLockObject() {
            return _lockObject;
        }

        Map<String, IMarker> getUidsMap() {
            return _uidsMap;
        }
    }

    public EvictionReplicationsMarkersRepository() {
        _segments = new EvictionReplicationsMarkersSegment[NUM_OF_IMARKERS_REPOSITORY_SEGMENTS];
        for (int i = 0; i < NUM_OF_IMARKERS_REPOSITORY_SEGMENTS; i++)
            _segments[i] = new EvictionReplicationsMarkersSegment();
        _size = new AtomicInteger();
    }

    @Override
    public void insert(String uid, IMarker marker, boolean alreadyLocked) {
        boolean newUid;
        if (!alreadyLocked) {
            Object lockObject = getLockObject(uid);
            try {
                synchronized (lockObject) {
                    newUid = insert(uid, marker);
                }
            } finally {
                releaseLockObject(lockObject);
            }
        } else
            newUid = insert(uid, marker);

        if (newUid)
            _size.incrementAndGet();
    }

    private boolean insert(String uid, IMarker marker) {
        return (_segments[getSegment(uid)].getUidsMap().put(uid, marker) == null);
    }

    @Override
    public boolean isEntryEvictable(String uid, boolean alreadyLocked) {
        if (!alreadyLocked) {
            Object lockObject = getLockObject(uid);
            try {
                synchronized (lockObject) {
                    return isEntryEvictable(uid);
                }
            } finally {
                releaseLockObject(lockObject);
            }

        } else
            return isEntryEvictable(uid);
    }

    private boolean isEntryEvictable(String uid) {
        IMarker marker = getMarkerUnsafe(uid);
        return marker == null || marker.isMarkerReached();
    }

    private IMarker getMarkerUnsafe(String uid) {
        return _segments[getSegment(uid)].getUidsMap().get(uid);
    }

    @Override
    public Object getLockObject(String uid) {
        return _segments[getSegment(uid)].getLockObject();
    }

    @Override
    public void releaseLockObject(Object lockObject) {
    }

    /**
     * reap unsed (i.e. reached) markers
     *
     * @return number of reapred markers
     */
    @Override
    public int reapUnused() {
        int res = 0;
        for (EvictionReplicationsMarkersSegment segment : _segments) {
            if (size() == 0)
                return res;
            res += reapUnused(segment);
        }
        return res;
    }

    private int reapUnused(EvictionReplicationsMarkersSegment segment) {
        int res = 0;
        try {
            synchronized (segment.getLockObject()) {
                if (segment.getUidsMap().isEmpty())
                    return 0;
                for (Iterator<IMarker> iter = segment.getUidsMap().values().iterator(); iter.hasNext(); ) {
                    IMarker marker = iter.next();
                    if (marker.isMarkerReached()) {
                        iter.remove();
                        _size.decrementAndGet();
                        res++;
                    }
                }

            }
        } finally {
            releaseLockObject(segment.getLockObject());
        }
        return res;

    }

    @Override
    public IMarker getMarker(String uid) {
        Object lockObject = getLockObject(uid);
        try {
            synchronized (lockObject) {
                return getMarkerUnsafe(uid);
            }
        } finally {
            releaseLockObject(lockObject);
        }
    }

    @Override
    public int size() {
        return _size.get();
    }

    private static int getSegment(String uid) {
        return Math.abs((uid.hashCode()) % NUM_OF_IMARKERS_REPOSITORY_SEGMENTS);
    }
}

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

import com.gigaspaces.internal.utils.concurrent.UncheckedAtomicIntegerFieldUpdater;
import com.gigaspaces.internal.utils.concurrent.UncheckedAtomicReferenceFieldUpdater;
import com.gigaspaces.server.eviction.EvictableServerEntry;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * mostly concurrent chain segment can be used for LRU and FIFO eviction insertion is totally
 * non-blocking removal locks the specific node + its previous in order to keep balanced list, we
 * use segments in order to minimize contention
 *
 * @author Yechiel Feffer
 * @since 7.0
 */
@com.gigaspaces.api.InternalApi
public class ChainSegment implements IEvictionChain {
    //unsafe means no protection against same-entry conflicts like remove & touch concurrently
    //when unsafe = false we can avoid some touching of volatile vars
    private boolean _unSafe;
    //even for safe impl, can touch be unsafe, i.e. occurs with remove ?
    private final boolean _unsafeTouch = true;

    private final LRUInfo _head;  //insert to head
    private final LRUInfo _tail;  //evict from fail
    private final short _segment;

    public ChainSegment(int segment) {
        this(true /*unsafe*/, segment);
    }

    public ChainSegment(boolean unsafe, int segment) {
        _segment = (short) segment;
        _head = new LRUInfo(_segment, null, null, null);
        _tail = new LRUInfo(_segment, null, _head, null);
        _head.setBwd(_tail);
        _head.nodeInsertionEnded();
        _tail.nodeInsertionEnded();

        _unSafe = unsafe;
    }

    @Override
    public void insert(EvictableServerEntry entry) {
        LRUInfo newnode = new LRUInfo(_segment, entry, _head, null);
        entry.setEvictionPayLoad(newnode);
        insertNode(newnode);
    }

    private void insertNode(LRUInfo newnode) {
        newnode.setDataWasInserted();
        while (true) {//insertion loop
            LRUInfo cur_first = _head.getBwd();
            newnode.setBwd(cur_first);
            if (_head.casBwd(cur_first, newnode)) {
                cur_first.setFwd(newnode);
                if (cur_first.isNodeRemoved()) {
                    //nodes that points at the head are not disconnected, maybe cur_first
                    //is removed so i may need to disconnect it
                    disconnect_node(cur_first);
                }
                newnode.nodeInsertionEnded();
                break;
            }
        }
    }

    @Override
    public boolean remove(EvictableServerEntry entry) {
        LRUInfo nodetodelete = (LRUInfo) entry.getEvictionPayLoad();

        //logical deletion
        if (!nodetodelete.setDataDeleted(_unSafe))
            return false;


        if (removeNode(nodetodelete)) {
            nodetodelete.setDataNull();
            return true;
        }
        if (((LRUInfo) entry.getEvictionPayLoad()) != nodetodelete) {
            nodetodelete = (LRUInfo) entry.getEvictionPayLoad();
            removeNode(nodetodelete);
        }
        nodetodelete.setDataNull();
        return true;
    }

    //return true if actually removed the node itself
    private boolean removeNode(LRUInfo nodetodelete) {
        //NOTE- a node that is pointing at the head is not removed just marked
        if (!nodetodelete.setNodeRemoved(_unSafe || _unsafeTouch))
            return false;

        //do a pointer manipulation -effort to remove
        disconnect_node(nodetodelete);

        return true;
    }

    private boolean disconnect_node(LRUInfo nodetodrop) {
        if (nodetodrop.isDisconnected() || nodetodrop.getFwd() == _head)
            return true;

        synchronized (nodetodrop) {
            if (nodetodrop.isDisconnected() || nodetodrop.getFwd() == _head)
                return true;
            //get my prev
            while (true) {
                LRUInfo prev = nodetodrop.getBwd();
                LRUInfo myfwd = nodetodrop.getFwd();

                synchronized (prev) {
                    if (prev != nodetodrop.getBwd())
                        continue;

                    prev.setFwd(myfwd);
                    myfwd.setBwd(prev);
                }
                nodetodrop.setDisconnected();
                return true;
            }

        }

    }

    public boolean touch(EvictableServerEntry entry) {
        LRUInfo lruInfo = (LRUInfo) entry.getEvictionPayLoad();
        if (lruInfo == null || !lruInfo.dataWasInserted())
            return true;
        if (lruInfo.getFwd() == _head && !lruInfo.isDataDeleted())
            return true; //i am already first

        boolean res = removeNode(lruInfo);
        if (!res && !_unSafe && !_unsafeTouch) {
            throw new RuntimeException("internal error in touch-entry not found id=" + entry);
        }
        if (!res)
            return false;

        if ((_unSafe || _unsafeTouch) && lruInfo.isDataDeleted())
            return false; //entry deleted meanwhile

        LRUInfo newnode = new LRUInfo(lruInfo, _head, null); //status object is always copied
        insertNode(newnode);
        entry.setEvictionPayLoad(newnode);

        if ((_unSafe || _unsafeTouch) && newnode.isDataDeleted()) {
            lruInfo = (LRUInfo) entry.getEvictionPayLoad();
            if (removeNode(lruInfo))
                return false;
        }
        return true;  //success

    }


    public boolean isEvictable(EvictableServerEntry entry) {
        LRUInfo lruInfo = (LRUInfo) entry.getEvictionPayLoad();
        return lruInfo.isDataEvictable();

    }

    private boolean isEmpty() {
        LRUInfo bk = _head.getBwd();

        return (bk == _tail || (bk.isDataDeleted() && bk.getBwd() == _tail));

    }


    public Iterator<EvictableServerEntry> evictionCandidates() {
        if (isEmpty())
            return null;
        return new ChainIter(this);
    }


    private static class ChainIter implements Iterator<EvictableServerEntry> {
        private LRUInfo _cur;
        LRUInfo _upto;
        private LRUInfo _next;
        final private ChainSegment _segment;

        ChainIter(ChainSegment segment) {
            _cur = segment._tail;
            _upto = segment._head.getBwd();
            _segment = segment;
        }

        public boolean hasNext() {
            if (_upto == _segment._tail)
                return false;
            LRUInfo nextnd = _cur.getFwd();
            while (true) {
                if (nextnd == null || nextnd == _segment._head) {
                    _next = null;
                    return false;
                }
                if (nextnd != _segment._tail && nextnd.isNodeRelevantForEviction()) {
                    _next = nextnd;
                    return true;
                }
                if (nextnd == _upto)
                    return false;
                nextnd = nextnd.getFwd();
            }
        }


        public EvictableServerEntry next() {
            if (_next == null && !hasNext())
                return null;

            _cur = _next;
            _next = null;
            return _cur.lruData._data;
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    //call it when no other action
    public void monitor() {
        int itersf = 0;
        int itersb = 0;
        int nonbalancedf = 0;
        int nonbalancedb = 0;
        int deletedf = 0;
        int deletedb = 0;
        int deletedInsidef = 0;
        int deletedInsideb = 0;

        for (LRUInfo basef = _tail; basef != null; basef = basef.getFwd()) {
            itersf++;
            if (basef.isNodeRemoved()) {
                deletedf++;
                continue;
            }
            if (basef != _tail) {
                LRUInfo myback = basef.backRef;
                if (myback.isNodeRemoved() && basef != _head) {
                    deletedInsidef++;
                    continue;
                }
                if (myback.fwdRef != basef) {
                    nonbalancedf++;
                    continue;
                }
            }
            if (basef != _head) {
                LRUInfo myf = basef.fwdRef;
                if (myf.isNodeRemoved() && myf.getFwd() != _head) {
                    deletedInsidef++;
                    continue;
                }
                if (myf.backRef != basef) {
                    nonbalancedf++;
                    continue;
                }
            }
        }
        for (LRUInfo baseb = _head; baseb != null; baseb = baseb.getBwd()) {
            itersb++;
            if (baseb.isNodeRemoved()) {
                deletedb++;
                continue;
            }
            if (baseb != _tail) {
                LRUInfo myback = baseb.backRef;
                if (myback.isNodeRemoved() && baseb != _head) {
                    deletedInsideb++;
                    continue;
                }
                if (myback.fwdRef != baseb) {
                    nonbalancedb++;
                    continue;
                }
            }
            if (baseb != _head) {
                LRUInfo myf = baseb.fwdRef;
                if (myf.isNodeRemoved() && myf.getFwd() != _head) {
                    deletedInsidef++;
                    continue;
                }
                if (myf.backRef != baseb) {
                    nonbalancedb++;
                    continue;
                }
            }
        }

        System.out.println("Seg= " + _segment + " itersf=" + itersf + " itersb=" + itersb +
                " nonbalancedf=" + nonbalancedf + " nonbalancedb=" + nonbalancedb +
                " deletedf=" + deletedf + " deletedb=" + deletedb +
                " deletedInsidef=" + deletedInsidef + " deletedInsideb=" + deletedInsideb);
    }


    static class LRUInfo {
        private static class Data {
            //data status values
            public final static int VALID = 0;
            public final static int DELETED = 1;

            private volatile int _dataStatus = VALID;

            private static final AtomicIntegerFieldUpdater<Data> dataStatusUpdater = UncheckedAtomicIntegerFieldUpdater.newUpdater(Data.class, "_dataStatus");

            private boolean _wasInserted; //this data was in the chain

            private EvictableServerEntry _data;

            Data(EvictableServerEntry data) {
                _data = data;
            }

            boolean isValid() {
                return _dataStatus == VALID;
            }

            boolean setDeleted(boolean unsafe) {//note- pinning state can only occur when unsafe = false
                if (unsafe)
                    return dataStatusUpdater.compareAndSet(this, VALID, DELETED);

                _dataStatus = DELETED;
                return true;

            }

            boolean isDeleted() {
                return _dataStatus == DELETED;
            }


            int getDataStatus() {
                return _dataStatus;
            }

            void setDataNull() {
                _data = null;
            }

            boolean wasInserted() {
                return _wasInserted;
            }

            void setWasInserted() {
                _wasInserted = true;
            }


        }

        final short _segment;

        //info status values
        public final static int INSERTING = 0;
        public final static int INSERTED = 1;
        public final static int REMOVED = 2; //node removed

        volatile int _infoStatus = INSERTING;
        private static final AtomicIntegerFieldUpdater<LRUInfo> infoStatusUpdater = UncheckedAtomicIntegerFieldUpdater.newUpdater(LRUInfo.class, "_infoStatus");

        //node ( removed) is also disconnected from list
        private boolean _disconnected;
        private final Data lruData;

        private volatile LRUInfo fwdRef;  //forward-to head
        private volatile LRUInfo backRef;  //backward-to tail

        private static final AtomicReferenceFieldUpdater<LRUInfo, LRUInfo> bwdUpdater = UncheckedAtomicReferenceFieldUpdater.newUpdater(LRUInfo.class, LRUInfo.class, "backRef");

        LRUInfo(short segment, EvictableServerEntry data, LRUInfo fwd, LRUInfo bwd) {
            lruData = new Data(data);
            if (fwd != null) //save touching volatile
                fwdRef = fwd;
            if (bwd != null) //save touching volatile
                backRef = bwd;
            _segment = segment;
        }

        LRUInfo(LRUInfo other, LRUInfo fwd, LRUInfo bwd) {
            lruData = other.lruData;
            if (fwd != null) //save touching volatile
                fwdRef = fwd;
            if (bwd != null) //save touching volatile
                backRef = bwd;
            _segment = other.getSegment();
        }

        public short getSegment() {
            return _segment;
        }

        boolean casBwd(LRUInfo expect, LRUInfo update) {
            return bwdUpdater.compareAndSet(this, expect, update);
        }

        void setFwd(LRUInfo update) {
            fwdRef = update;
        }

        void setBwd(LRUInfo update) {
            backRef = update;
        }

        LRUInfo getFwd() {
            return fwdRef;
        }

        LRUInfo getBwd() {
            return backRef;
        }

        boolean setDataDeleted(boolean unsafe) {
            return lruData.setDeleted(unsafe);
        }

        boolean isDataDeleted() {
            return lruData.isDeleted();

        }

        boolean setNodeRemoved(boolean unsafe) {
            if (unsafe)
                return infoStatusUpdater.compareAndSet(this, INSERTED, REMOVED);

            _infoStatus = REMOVED;
            return true;

        }

        boolean isNodeRemoved() {
            return _infoStatus == REMOVED;
        }

        void setNodeUnremoved() {
            _infoStatus = INSERTING;
        }

        boolean isNodeInserted() {
            return _infoStatus == INSERTED;
        }

        boolean isNodeBeingInserted() {
            return _infoStatus == INSERTING;
        }

        int getDataStatus() {
            return lruData.getDataStatus();
        }

        boolean isDataEvictable() {
            return lruData.isValid();
        }

        void nodeInsertionEnded() {
            _infoStatus = INSERTED;

        }

        boolean isDisconnected() {
            return _disconnected;
        }

        boolean setDisconnected() {//should be called under lock
            return _disconnected = true;
        }

        boolean isNodeRelevantForEviction() {
            return _infoStatus == INSERTED && lruData.isValid();
        }

        boolean dataWasInserted() {
            return lruData.wasInserted();
        }

        void setDataWasInserted() {
            lruData.setWasInserted();
        }

        void setDataNull() {
            lruData.setDataNull();
        }
    }
}

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


package com.j_spaces.core.cache;

import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.gigaspaces.time.SystemTime;

import java.util.concurrent.ConcurrentHashMap;

/**
 * TODO	add Javadoc
 *
 * @author Yechiel Fefer
 * @version 1.0
 * @since 5.0
 */

/**
 * contanis information regarding terminatiing transactions which have fifo entries - the seq number
 * of last terminating xtn and info regarding terminating entries
 */
@com.gigaspaces.api.InternalApi
public class TerminatingFifoXtnsInfo {
    final public static long UNKNOWN_FIFO_XTN = -1;

    /**
     * the seq' number of latest fifo transaction termination, the number represents the latest
     * transaction currently terminating
     */
    private volatile long _latestTransactionTerminationNum;

    public ConcurrentHashMap<FifoXtnEntryInfo, FifoXtnEntryInfo> _terminatingXtnsEntries;

    public TerminatingFifoXtnsInfo() {
        _terminatingXtnsEntries = new ConcurrentHashMap<FifoXtnEntryInfo, FifoXtnEntryInfo>();
    }

    public long getLatestTTransactionTerminationNum() {
        return _latestTransactionTerminationNum;
    }

    //NOTE- must be called when xtns are locked !!!!!!!!
    public void setLatestTransactionTerminationNum(long xtnTerminationNum) {
        _latestTransactionTerminationNum = xtnTerminationNum;
    }

    /**
     * create fifo xtn info for entry MUST be called  when entry is locked
     */
    public void setFifoCreationXtnInfoForEntry(IEntryHolder eh, long xtnNumber) {
        FifoXtnEntryInfo xe = new FifoXtnEntryInfo(eh.getUID(), eh.getSCN(), eh.getOrder());
        xe._creatingXtn = xtnNumber;
        xe.setTime();
        _terminatingXtnsEntries.put(xe, xe);

    }


    /**
     * update fifo xtn info for entry MUST be called  when entry is locked update write lock if
     * writeLock is true, else update read lock, also update the xtn which committed the entry
     */
    public void updateFifoXtnInfoForEntry(IEntryHolder eh, long xtnNumber, boolean writeLock, boolean entryWritingXtn) {
        FifoXtnEntryInfo newXe = new FifoXtnEntryInfo(eh.getUID(), eh.getSCN(), eh.getOrder());
        FifoXtnEntryInfo xe = _terminatingXtnsEntries.putIfAbsent(newXe, newXe);
        if (xe == null)
            xe = newXe;

        if (writeLock)
            xe._writeLockInfo = entryWritingXtn ? new FifoXtnWriteLockEntryInfo(xtnNumber, xtnNumber) :
                    new FifoXtnWriteLockEntryInfo(xtnNumber);
        else
            xe._terminatingXtnReadLock = xtnNumber;

        xe.setTime();

    }


    /**
     * remove fifo xtn info for entry MUST be called  when entry is locked
     */
    public void removeFifoXtnInfoForEntry(IEntryHolder eh) {
        FifoXtnEntryInfo Xe = new FifoXtnEntryInfo(eh.getUID(), eh.getSCN(), eh.getOrder());
        _terminatingXtnsEntries.remove(Xe);

    }


    public FifoXtnEntryInfo getFifoEntryXtnInfo(IEntryHolder eh) {
        FifoXtnEntryInfo Xe = new FifoXtnEntryInfo(eh.getUID(), eh.getSCN(), eh.getOrder());
        return _terminatingXtnsEntries.get(Xe);
    }

    public ConcurrentHashMap<FifoXtnEntryInfo, FifoXtnEntryInfo> getTerminatingXtnsEntries() {
        return _terminatingXtnsEntries;
    }

    /**
     * method returns true if xtn1 is sequentially GT xtn2
     *
     * @return true if xtn1 GT xtn2
     */
    public static boolean isSeqTransactionGT(long xtn1, long xtn2) {
        if (xtn1 != UNKNOWN_FIFO_XTN && xtn2 != UNKNOWN_FIFO_XTN &&
                (xtn1 > xtn2 || (xtn1 < xtn2 && xtn2 - xtn1 > 1000000 /*wraped around*/)))
            return true;
        else
            return false;
    }


    public static class FifoXtnEntryInfo {
        //xtn fifo information per entry

        //uid of entry terminating
        private String _UID;
        private long _scn; //scn of entry
        private int _order; //fifo order of entry

        //seq number of transaction in the system when the entry was created
        private long _creatingXtn;
        //seq number of xtn read-locked
        private long _terminatingXtnReadLock;
        //write lock info
        private volatile FifoXtnWriteLockEntryInfo _writeLockInfo;
        //time of last change
        private volatile long _changeTime;

        public FifoXtnEntryInfo(String UID, long scn, int order) {
            _UID = UID;
            _scn = scn;
            _order = order;
            _creatingXtn = _terminatingXtnReadLock = UNKNOWN_FIFO_XTN;
        }

        public void setTime() {
            _changeTime = SystemTime.timeMillis();
        }

        public long getCreatingXtn() {
            return _creatingXtn;
        }

        public long getTerminatingXtnReadLock() {
            return _terminatingXtnReadLock;
        }

        public long getTerminatingXtnWriteLock() {
            return _writeLockInfo != null ? _writeLockInfo.getTerminatingXtnWriteLock() : UNKNOWN_FIFO_XTN;
        }

        public long getChangeTime() {
            return _changeTime;
        }

        public String getUid() {
            return _UID;
        }

        public long getEntryWriteXtnNumber() {
            return _writeLockInfo != null ? _writeLockInfo.getEntryWriteXtnNumber() : UNKNOWN_FIFO_XTN;
        }

        public int hashCode() {
            return _UID.hashCode();
        }

        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            FifoXtnEntryInfo otherInfo = (FifoXtnEntryInfo) other;
            return
                    _scn == otherInfo._scn &&
                            _order == otherInfo._order &&
                            _UID.equals(otherInfo._UID);

        }


    }

    public static class FifoXtnWriteLockEntryInfo {
        //write lock info as 1 object in order to achieve non-blocking read

        //seq number of xtn write-locked
        private long _terminatingXtnWriteLock;
        //the seq xtn which committed the entry
        private long _entryWriteXtnNumber;

        public FifoXtnWriteLockEntryInfo(long terminatingXtnWriteLock, long entryWriteXtnNumber) {
            _terminatingXtnWriteLock = terminatingXtnWriteLock;
            _entryWriteXtnNumber = entryWriteXtnNumber;
        }

        public FifoXtnWriteLockEntryInfo(long terminatingXtnWriteLock) {
            this(terminatingXtnWriteLock, UNKNOWN_FIFO_XTN);
        }

        public long getTerminatingXtnWriteLock() {
            return _terminatingXtnWriteLock;
        }

        public long getEntryWriteXtnNumber() {
            return _entryWriteXtnNumber;
        }

    }


}

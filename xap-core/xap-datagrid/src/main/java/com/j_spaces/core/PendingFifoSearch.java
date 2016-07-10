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


package com.j_spaces.core;

import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.j_spaces.core.fifo.FifoBackgroundRequest;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

/**
 * this class is used for fifo templates such object is created for a non-uid-based fifo templates
 * in order to ensure that 1. while initial-scan searching the entries ignore ending transactions in
 * order not to consider entries which are in the middle of transaction 2. reject entries which  are
 * not part of the initial-scan, they are kept in a TreeMap for processing after initial-scan is
 * terminated
 *
 * @author Yechiel Fefer
 * @since 5.0
 */
@com.gigaspaces.api.InternalApi
public class PendingFifoSearch {
    //phases of initial scan
    static int INITIAL_SCAN_ACTIVE = 0;  //the template is scanning
    static int INITIAL_SCAN_NOTACTIVE = 1;

    //stage of initial_scan, or none
    private int _status;

    private TreeMap<IEntryHolder, FifoBackgroundRequest> _rejectedFifoEntries = null;

    private boolean _needNotifyTermination;
    private final boolean _memorySpace;
    final private static FifoEntriesComparator m_Comparator =
            new FifoEntriesComparator();


    public PendingFifoSearch(boolean memorySpace) {
        _status = INITIAL_SCAN_ACTIVE;
        _memorySpace = memorySpace;
    }


    //add a rejected entry
    public synchronized void addRejectedEntry(IEntryHolder eh, FifoBackgroundRequest red) {

        if (_rejectedFifoEntries == null)
            _rejectedFifoEntries = new TreeMap<IEntryHolder, FifoBackgroundRequest>(m_Comparator);

        _rejectedFifoEntries.put(eh, red);

    }

    public synchronized boolean anyRejectedEntries() {
        return _rejectedFifoEntries != null && !_rejectedFifoEntries.isEmpty();
    }


    public synchronized ArrayList<FifoBackgroundRequest> getRejectedEntries() {
        if (_rejectedFifoEntries == null)
            return null;
        ArrayList<FifoBackgroundRequest> res = new ArrayList<FifoBackgroundRequest>();

        Iterator<Map.Entry<IEntryHolder, FifoBackgroundRequest>> titer = _rejectedFifoEntries.entrySet().iterator();
        while (titer.hasNext()) {
            Map.Entry<IEntryHolder, FifoBackgroundRequest> me = titer.next();
            IEntryHolder eh = me.getKey();
            if (_memorySpace && eh.isDeleted())
                continue;
            res.add(me.getValue());
        }


        return res;
    }

    /**
     * notify (if requested) when the object is not active any more- i.e. not accumulating fifo
     * events for the template
     */
    public synchronized void notifyNonActiveIfNeedTo() {
        _status = INITIAL_SCAN_NOTACTIVE;
        if (_needNotifyTermination) {
            this.notify();
        }
    }

    /**
     * wait for status to become non-active i.e. not accumulating fifo events for the template
     */
    public synchronized void waitForNonActiveStatus() {
        try {
            if (_status == INITIAL_SCAN_NOTACTIVE)
                return;
            _needNotifyTermination = true;
            this.wait(5000);
        } catch (InterruptedException e) {
            // we keep the interrupt status
            Thread.currentThread().interrupt();
        }
    }


}

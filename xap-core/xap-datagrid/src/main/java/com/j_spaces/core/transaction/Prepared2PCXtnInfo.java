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

package com.j_spaces.core.transaction;

import com.gigaspaces.internal.server.space.SpaceEngine;
import com.j_spaces.core.XtnEntry;

/**
 * 2pc prepared xtns info
 *
 * @author Yechiel
 * @since 11.0
 */
@com.gigaspaces.api.InternalApi
public class Prepared2PCXtnInfo {

    private final XtnEntry _xtnEntry;
    private final long _prepareTime;
    private long _uptoTime;
    private int _numExtentions;  //number of times the lease manager exteded it
    private long _busyInTmStartTime;

    public Prepared2PCXtnInfo(XtnEntry xtnEntry, long basicTimeForPrepare) {
        _xtnEntry = xtnEntry;
        _prepareTime = System.currentTimeMillis();
        _uptoTime = _prepareTime + basicTimeForPrepare;
    }

    public long getUptoTime() {
        return _uptoTime;
    }

    public XtnEntry getXtnEntry() {
        return _xtnEntry;
    }

    public boolean isExpiredPrepareTime(long current) {
        return current > _uptoTime;
    }

    public boolean extendIfPossible(SpaceEngine engine) {
        if (!TransactionHandler.isSupportsPreparedXtnsExtentionTime())
            return false;
        if (_numExtentions >= TransactionHandler.getMaxNumberOfPreaparedXtnExtentions())
            return false;
        if (_busyInTmStartTime != 0)
            return false; //another LM thread is checking


        //perform callback to mahalo. note- should be done in pool thread and not directly
        CheckXtnStatusInTmBusPackect p = new CheckXtnStatusInTmBusPackect(_xtnEntry.getServerTransaction());
        engine.getProcessorWG().enqueueBlocked(p);
        //wait for completion
        long str = System.currentTimeMillis();
        _busyInTmStartTime = str;
        long left = TransactionHandler.getTimeToWaitForTm();
        synchronized (p.getNotifyObj()) {
            try {
                while (true) {
                    if (!p.isHasAnswer())
                        p.getNotifyObj().wait(left);
                    if (p.isHasAnswer())
                        break;
                    left = left - (System.currentTimeMillis() - str);
                    if (left <= 0)
                        break;
                }
            } catch (InterruptedException ex) {
            }
        }

        if (!p.isNotAbortedLiveTxn())
            return false;

        _numExtentions++;
        _uptoTime += TransactionHandler.getExtentionTimeForPreparedXtn();
        _busyInTmStartTime = 0;
        return true;
    }

}

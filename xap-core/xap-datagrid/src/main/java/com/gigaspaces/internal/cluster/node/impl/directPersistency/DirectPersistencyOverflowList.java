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

package com.gigaspaces.internal.cluster.node.impl.directPersistency;


import java.util.Iterator;

/**
 * the list of elements overflowen when # in memory exceeds a defined minimum enables locating
 * groups of elements bwteen redokey values
 *
 * @author yechielf
 * @since 10.2
 */
@com.gigaspaces.api.InternalApi
public class DirectPersistencyOverflowList {

//NOTE- only the bacjground thread is using this list

    private final IDirectPersistencySyncHandler _main;
    private int _numOverFlowen;
    private DirectPersistencyOverflowListSegment _oldest;
    private long _oldestSeq;
    private long _currentSeq;
    private DirectPersistencyOverflowListSegment _current; //the most recent segment


    DirectPersistencyOverflowList(IDirectPersistencySyncHandler main) {
        _main = main;
        reset();
    }

    public boolean isEmpty() {
        return _numOverFlowen == 0;
    }


    //overflow op-objects from mem to disk/ssd
    public int moveToOverflow(Iterator<IDirectPersistencyOpInfo> iter, int numberToOverflow) {
        int res = 0;
        for (int i = 0; i < numberToOverflow && iter.hasNext(); i++) {
            if (_current.isFull())
                setNewCurrent();
            IDirectPersistencyOpInfo cur = iter.next();
            if (cur.getRedoKey() <= _main.getLastConfirmed()) {//we can just delete this one-no need to overflow it
                //remove this record from disk and memory
                _main.getListHandler().getIoHandler().remove(cur);
                iter.remove();
                continue;
            }
            //update the op record in order to persist the redokey
            _main.getListHandler().getIoHandler().update(cur);
            _current.add(_main, cur);
            _numOverFlowen++;
            res++;
            iter.remove();
        }
        return res;
    }

    private void setNewCurrent() {
        if (_current != null)
            _current.persist(_main);
        _current = new DirectPersistencyOverflowListSegment(_main.getCurrentGenerationId(), ++_currentSeq);
        if (_oldest == null) {
            _oldest = _current;
            _oldestSeq = _currentSeq;
        }
    }

    public int syncToRedologConfirmation(long confirmed) {//confirmation arrived remove from overflow if relevant
        int res = 0;
        while (true) {
            if (isEmpty())
                return res;
            DirectPersistencyOverflowListSegment workingSegment = _oldest;
            int cur = workingSegment.syncToRedologConfirmation(_main, confirmed);
            res += cur;
            _numOverFlowen -= cur;

            if (!workingSegment.isEmpty())
                return res;

            //segment is empty-remove it
            workingSegment.remove(_main);
            if (isEmpty()) {
                reset();
                return res;
            }
            _oldestSeq++;
            if (_oldestSeq == _currentSeq) {
                _oldest = _current;
                continue; //no need to get from persistent
            }
            _oldest = _main.getListHandler().getIoHandler().getOverflowSegment(_main.getCurrentGenerationId(), _oldestSeq);
        }
    }


    private void reset() {
        _current = null;
        _oldest = null;
        _oldestSeq = -1;
        _currentSeq = -1;
        _numOverFlowen = 0;
        setNewCurrent();
    }


}

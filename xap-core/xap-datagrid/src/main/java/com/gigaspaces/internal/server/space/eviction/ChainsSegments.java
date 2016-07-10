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

import com.gigaspaces.server.eviction.EvictableServerEntry;
import com.j_spaces.kernel.SystemProperties;

import java.util.Iterator;

/**
 * segments. each segment is mostly concurrent chain. mostly concurrent chain segment can be used
 * for LRU and FIFO eviction insertion is totally non-blocking, and to a random segment removal
 * locks the specific node + its previous in order to keep balanced list segments are used to spread
 * contention
 *
 * @author Yechiel Feffer
 * @since 7.0
 */
@com.gigaspaces.api.InternalApi
public class ChainsSegments implements IEvictionChain {
    private final ChainSegment[] _segments;
    // counts the number of adds - used to spread the objects evenly between segments
    private int _addCounter = 0;
    // counts the number of scans - used to spread the start between segments
    private int _scanCounter = 0;

    public ChainsSegments() {
        this(false /*unsafe*/, 100 /*evictionThreshold*/);
    }

    public ChainsSegments(boolean unsafe, int evictionThreshold) {
        this(unsafe, evictionThreshold, Integer.getInteger(SystemProperties.ENGINE_LRU_SEGMENTS, 0));
    }

    public ChainsSegments(boolean unsafe, int evictionThreshold, int numOfSegments) {
        //unsafe means no protection against same-entry conflicts like remove & touch concurrently
        //when unsafe = false we can avoid some touching of volatile vars

        // if set to 0 - use the default
        if (numOfSegments == 0)
            numOfSegments = SystemProperties.ENGINE_LRU_SEGMENTS_DEFAULT;

        _segments = new ChainSegment[numOfSegments];
        for (int i = 0; i < numOfSegments; i++)
            _segments[i] = new ChainSegment(unsafe, i);
    }

    private int getNumSegments() {
        return _segments.length;
    }

    ChainSegment getSegment(int num) {
        return _segments[num];
    }

    @Override
    public void insert(EvictableServerEntry entry) {
        //select a random segment to insert to
        int seg = drawSegmentNumber(true /*add*/);
        _segments[seg].insert(entry);

    }

    //draw a segment number for insertions/scans
    private int drawSegmentNumber(boolean add) {
        int tnum = (int) Thread.currentThread().getId();
        if (tnum % getNumSegments() == 0)
            tnum++;
        return add ? Math.abs((tnum * _addCounter++) % getNumSegments()) : Math.abs((tnum * _scanCounter++) % getNumSegments());
    }

    @Override
    public boolean remove(EvictableServerEntry entry) {
        return getSegment(entry).remove(entry);
    }

    @Override
    public boolean touch(EvictableServerEntry entry) {
        ChainSegment.LRUInfo node = (ChainSegment.LRUInfo) entry.getEvictionPayLoad();
        if (node == null || !node.isNodeInserted())
            return false;
        return _segments[node.getSegment()].touch(entry);
    }


    @Override
    public boolean isEvictable(EvictableServerEntry entry) {
        return getSegment(entry).isEvictable(entry);
    }

    private ChainSegment getSegment(EvictableServerEntry entry) {
        ChainSegment.LRUInfo node = (ChainSegment.LRUInfo) entry.getEvictionPayLoad();
        return _segments[node.getSegment()];
    }

    public Iterator<EvictableServerEntry> evictionCandidates() {//NOTE- hasnext has to be called before next
        return new ChainIter(this);
    }


    /**
     * NOTE - hasnext has to be called before next
     */
    private static class ChainIter implements Iterator<EvictableServerEntry> {
        private static final int UNITITIALIZED = -1;
        private static final int TERMINATED = -2;

        int _next = UNITITIALIZED; //the last position used for next() call that returned true
        final private ChainsSegments _segments;
        final private Iterator<EvictableServerEntry>[] _iters;

        ChainIter(ChainsSegments segments) {
            _segments = segments;
            _iters = new Iterator[_segments.getNumSegments()];
        }

        public boolean hasNext() {
            if (_next == TERMINATED)
                return false;
            if (_next == UNITITIALIZED)
                _next = _segments.drawSegmentNumber(false /*add*/);
            else
                _next++;

            int tries = 0;
            while (true) {
                if (_next == _segments.getNumSegments())
                    _next = 0;
                if (_iters[_next] == null) {
                    _iters[_next] = _segments.getSegment(_next).evictionCandidates();
                    if (_iters[_next] == null)
                        _iters[_next] = this;
                }
                if (_iters[_next] != this) {
                    if (_iters[_next].hasNext())
                        return true;

                    _iters[_next] = this; //signal for no use
                }
                if (++tries >= _segments.getNumSegments()) {
                    _next = TERMINATED;
                    return false;
                }
                _next++;
            }
        }


        public EvictableServerEntry next() {
            if (_next == TERMINATED || _next == UNITITIALIZED)
                throw new RuntimeException("successful hasnext() should be called");

            return _iters[_next].next();

        }

        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    //call it when no other action
    public void monitor() {
        for (int i = 0; i < _segments.length; i++)
            _segments[i].monitor();

    }
}

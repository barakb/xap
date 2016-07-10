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

package com.gigaspaces.client.iterator.internal;

import com.gigaspaces.internal.client.QueryResultTypeInternal;
import com.gigaspaces.internal.query.IPartitionResultMetadata;
import com.gigaspaces.internal.transport.TemplatePacketFactory;
import com.j_spaces.core.UidQueryPacket;

import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Set;

/**
 * MultipleUidsPerPartitionList keeps the uids grouped by the partition they came from. <br> The
 * order is the following:
 *
 * partition1, uids from partition1, partition2, uids from partition2, etc.
 *
 * @author anna
 * @since 7.0
 */
@com.gigaspaces.api.InternalApi
public class MultipleUidsPerPartitionList {
    private int _numOfPartitions;
    private final Set<Object> _internalList;

    public MultipleUidsPerPartitionList() {
        _internalList = new LinkedHashSet<Object>();
    }

    public int size() {
        return _internalList.size() - _numOfPartitions;
    }

    public void clear() {
        _internalList.clear();
        _numOfPartitions = 0;
    }

    public boolean addIfNew(String uid) {
        return _internalList.add(uid);
    }

    public void remove(String uid) {
        _internalList.remove(uid);
    }

    public void startPartition(IPartitionResultMetadata partition) {
        _numOfPartitions++;
        _internalList.add(partition);
    }

    public UidQueryPacket buildQueryPacket(int bufferSize, QueryResultTypeInternal resultType) {
        if (size() == 0)
            return null;

        int minBufferSize = Math.min(bufferSize, size());
        String[] uids = new String[minBufferSize];
        int idx = 0;
        LinkedList<IPartitionResultMetadata> partitions = new LinkedList<IPartitionResultMetadata>();

        int lastPartitionCount = 0;
        for (Iterator<Object> itor = _internalList.iterator(); itor.hasNext() && idx < minBufferSize; ) {
            Object o = itor.next();

            // if uid - add to array
            if (o.getClass() == String.class) {
                itor.remove();
                uids[idx++] = (String) o;
                lastPartitionCount++;
            } else {
                // remove empty partitions, than no longer have any uids
                if (lastPartitionCount == 0 && !partitions.isEmpty())
                    partitions.removeLast();

                partitions.add((IPartitionResultMetadata) o);
                lastPartitionCount = 0;
            }
        }

        Object routing = partitions != null && partitions.size() == 1 ? partitions.get(0).getPartitionId() : null;

        UidQueryPacket queryPacket = (UidQueryPacket) TemplatePacketFactory.createUidsPacket(uids, resultType, false);
        queryPacket.setRouting(routing);
        return queryPacket;
    }
}

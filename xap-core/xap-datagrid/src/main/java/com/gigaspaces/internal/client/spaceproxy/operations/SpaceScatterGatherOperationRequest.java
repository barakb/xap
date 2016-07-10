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

package com.gigaspaces.internal.client.spaceproxy.operations;

import com.gigaspaces.internal.remoting.routing.partitioned.OperationResult;
import com.gigaspaces.internal.remoting.routing.partitioned.ScatterGatherPartitionInfo;
import com.gigaspaces.internal.remoting.routing.partitioned.ScatterGatherRemoteOperationRequest;

/**
 * @author Niv Ingberg
 * @since 9.0.0
 */
public abstract class SpaceScatterGatherOperationRequest<TResult extends SpaceOperationResult> extends SpaceOperationRequest<TResult> implements ScatterGatherRemoteOperationRequest<TResult> {
    private static final long serialVersionUID = 1L;

    protected ScatterGatherPartitionInfo _partitionInfo;

    @Override
    public SpaceScatterGatherOperationRequest<TResult> createCopy(int targetPartitionId) {
        SpaceScatterGatherOperationRequest<TResult> copy = (SpaceScatterGatherOperationRequest<TResult>) super.createCopy(targetPartitionId);
        copy._partitionInfo = new ScatterGatherPartitionInfo(targetPartitionId);
        return copy;
    }

    @Override
    public ScatterGatherPartitionInfo getPartitionInfo() {
        return _partitionInfo;
    }

    protected int[] scatter(int[] data) {
        if (data == null)
            return null;
        int[] partitionData = new int[_partitionInfo.size()];
        for (int i = 0; i < partitionData.length; i++)
            partitionData[i] = data[_partitionInfo.getQuick(i)];
        return partitionData;
    }

    protected long[] scatter(long[] data) {
        if (data == null)
            return null;
        long[] partitionData = new long[_partitionInfo.size()];
        for (int i = 0; i < partitionData.length; i++)
            partitionData[i] = data[_partitionInfo.getQuick(i)];
        return partitionData;
    }

    protected <T> T[] scatter(T[] data, T[] partitionData) {
        if (data == null)
            return null;
        for (int i = 0; i < partitionData.length; i++)
            partitionData[i] = data[_partitionInfo.getQuick(i)];
        return partitionData;
    }

    public void gather(int[] data, int[] partitionData) {
        for (int i = 0; i < partitionData.length; i++)
            data[_partitionInfo.getQuick(i)] = partitionData[i];
    }

    public <T> void gather(T[] data, T[] partitionData) {
        for (int i = 0; i < partitionData.length; i++)
            data[_partitionInfo.getQuick(i)] = partitionData[i];
    }

    public void gather(OperationResult<Integer>[] data, int[] partitionData) {
        for (int i = 0; i < partitionData.length; i++)
            data[_partitionInfo.getQuick(i)] = new OperationResult<Integer>(partitionData[i], null);
    }

    public <T> void gather(OperationResult<T>[] data, Exception error) {
        for (int i = 0; i < _partitionInfo.size(); i++)
            data[_partitionInfo.getQuick(i)] = new OperationResult<T>(null, error);
    }

    public <T> void gather(T[] data, T partitionData) {
        for (int i = 0; i < _partitionInfo.size(); i++)
            data[_partitionInfo.getQuick(i)] = partitionData;
    }
}

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

package com.gigaspaces.internal.cluster.node.impl.replica;

import com.gigaspaces.internal.cluster.node.replica.ISpaceCopyReplicaParameters;
import com.gigaspaces.internal.cluster.node.replica.ISpaceCopyReplicaRequestContext;

@SuppressWarnings("deprecation")
@com.gigaspaces.api.InternalApi
public class SpaceCopyReplicaRequestContext
        implements ISpaceCopyReplicaRequestContext {
    private static final int DEFAULT_BATCH_SIZE = 500;
    private static final int DEFAULT_CONCURRENT_CONSUMERS = 1;
    private static final long DEFAULT_PROGRESS_TIMEOUT = Long.getLong("com.gs.replication.replicaProgressTimeout", 60000);

    private int _concurrentConsumers = DEFAULT_CONCURRENT_CONSUMERS;
    private int _fetchBatchSize = DEFAULT_BATCH_SIZE;
    private ISpaceCopyReplicaParameters _copyParameters;
    private Object _originUrl;
    private long _progressTimeoutMilis = DEFAULT_PROGRESS_TIMEOUT;

    public void setConcurrentConsumers(int concurrentConsumers) {
        _concurrentConsumers = concurrentConsumers;
    }

    public int getConcurrentConsumers() {
        return _concurrentConsumers;
    }

    public void setFetchBatchSize(int fetchBatchSize) {
        _fetchBatchSize = fetchBatchSize;
    }

    public int getFetchBatchSize() {
        return _fetchBatchSize;
    }

    public ISpaceCopyReplicaParameters getParameters() {
        return _copyParameters;
    }

    public void setParameters(ISpaceCopyReplicaParameters copyParameters) {
        _copyParameters = copyParameters;
    }

    public Object getOriginUrl() {
        return _originUrl;
    }

    public void setOriginUrl(Object originUrl) {
        _originUrl = originUrl;
    }

    public long getProgressTimeout() {
        return _progressTimeoutMilis;
    }

    public void setProgressTimeout(long progressTimeoutMilis) {
        _progressTimeoutMilis = progressTimeoutMilis;
    }

    @Override
    public String toString() {
        return "SpaceCopyReplicaRequestContext [getConcurrentConsumers()=" + getConcurrentConsumers()
                + ", getFetchBatchSize()=" + getFetchBatchSize() + ", getParameters()=" + getParameters()
                + ", getOriginUrl()=" + getOriginUrl() + ", getProgressTimeout()=" + getProgressTimeout() + "]";
    }


}

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

package com.gigaspaces.internal.cluster.node.impl.groups.async;

import com.gigaspaces.internal.cluster.node.impl.backlog.async.IReplicationAsyncBacklogBuilder;
import com.gigaspaces.internal.cluster.node.impl.backlog.async.IReplicationAsyncGroupBacklog;
import com.gigaspaces.internal.cluster.node.impl.config.DynamicAsyncSourceGroupConfigHolder;
import com.gigaspaces.internal.cluster.node.impl.config.DynamicSourceGroupConfigHolder;
import com.gigaspaces.internal.cluster.node.impl.config.SourceGroupConfig;
import com.gigaspaces.internal.cluster.node.impl.filters.IReplicationOutFilter;
import com.gigaspaces.internal.cluster.node.impl.groups.AbstractReplicationSourceGroupBuilder;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationSourceGroup;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationSourceGroupStateListener;
import com.gigaspaces.internal.cluster.node.impl.router.IReplicationRouter;
import com.gigaspaces.internal.utils.concurrent.IAsyncHandlerProvider;


@com.gigaspaces.api.InternalApi
public class AsyncReplicationSourceGroupBuilder
        extends AbstractReplicationSourceGroupBuilder<SourceGroupConfig> {

    private long _intervalMilis;
    private IAsyncHandlerProvider _asyncHandlerProvider;
    private int _batchSize;
    private IReplicationAsyncBacklogBuilder _backlogBuilder;
    private int _intervalOperations;

    public AsyncReplicationSourceGroupBuilder(DynamicAsyncSourceGroupConfigHolder groupConfig) {
        super(groupConfig);
    }

    public void setIntervalMilis(long intervalMilis) {
        _intervalMilis = intervalMilis;
    }

    public void setIntervalOperations(int intervalOperations) {
        _intervalOperations = intervalOperations;
    }

    public void setAsyncHandlerProvider(
            IAsyncHandlerProvider asyncHandlerProvider) {
        _asyncHandlerProvider = asyncHandlerProvider;
    }

    public void setBatchSize(int batchSize) {
        _batchSize = batchSize;
    }

    public void setBacklogBuilder(IReplicationAsyncBacklogBuilder backlogBuilder) {
        _backlogBuilder = backlogBuilder;
    }

    @Override
    protected IReplicationSourceGroup createGroupImpl(
            DynamicSourceGroupConfigHolder groupConfig,
            IReplicationRouter replicationRouter,
            IReplicationOutFilter outFilter,
            IReplicationSourceGroupStateListener stateListener) {
        IReplicationAsyncGroupBacklog groupBacklog = _backlogBuilder.createAsyncGroupBacklog(groupConfig);
        return new AsyncReplicationSourceGroup(groupConfig,
                replicationRouter,
                groupBacklog,
                replicationRouter.getMyLookupName(),
                outFilter,
                _batchSize,
                _intervalMilis,
                _intervalOperations,
                _asyncHandlerProvider,
                stateListener);
    }

    @Override
    public String toString() {
        return "AsyncReplicationSourceGroupBuilder [_groupConfig="
                + getGroupConfig() + ", _intervalMilis=" + _intervalMilis
                + ", _asyncHandlerProvider=" + _asyncHandlerProvider
                + ", _batchSize=" + _batchSize + ", _backlogBuilder="
                + _backlogBuilder + "]";
    }

}

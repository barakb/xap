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

package com.gigaspaces.internal.client.spaceproxy.router;

import com.gigaspaces.internal.cluster.SpaceProxyLoadBalancerType;
import com.gigaspaces.internal.remoting.routing.clustered.RemoteOperationsExecutorsClusterConfig;
import com.gigaspaces.internal.server.space.operations.SpaceOperationsCodes;
import com.gigaspaces.internal.utils.PropertiesUtils;
import com.j_spaces.core.Constants;

import java.util.Properties;

/**
 * @author Niv Ingberg
 * @since 9.0.1
 */
@com.gigaspaces.api.InternalApi
public class SpaceRemoteOperationsExecutorsClusterConfig implements RemoteOperationsExecutorsClusterConfig {
    private final long _activeServerLookupTimeout;
    private final long _activeServerLookupSamplingInterval;
    private final int _threadPoolSize;
    private SpaceProxyLoadBalancerType _loadBalancerType;
    private final int _numOfOperationsTypes;

    public SpaceRemoteOperationsExecutorsClusterConfig() {
        this(null);
    }

    public SpaceRemoteOperationsExecutorsClusterConfig(Properties properties) {
        this._activeServerLookupTimeout = PropertiesUtils.getLong(properties, Constants.SpaceProxy.Router.ACTIVE_SERVER_LOOKUP_TIMEOUT,
                Constants.SpaceProxy.Router.ACTIVE_SERVER_LOOKUP_TIMEOUT_DEFAULT);
        this._activeServerLookupSamplingInterval = PropertiesUtils.getLong(properties, Constants.SpaceProxy.Router.ACTIVE_SERVER_LOOKUP_SAMPLING_INTERVAL,
                Constants.SpaceProxy.Router.ACTIVE_SERVER_LOOKUP_SAMPLING_INTERVAL_DEFAULT);
        this._threadPoolSize = PropertiesUtils.getInteger(properties, Constants.SpaceProxy.Router.THREAD_POOL_SIZE,
                Runtime.getRuntime().availableProcessors() * 2);
        this._numOfOperationsTypes = SpaceOperationsCodes.NUM_OF_OPERATIONS;
        this._loadBalancerType = PropertiesUtils.getEnum(properties, Constants.SpaceProxy.Router.LOAD_BALANCER_TYPE,
                SpaceProxyLoadBalancerType.class, SpaceProxyLoadBalancerType.STICKY);
    }

    @Override
    public long getActiveServerLookupTimeout() {
        return _activeServerLookupTimeout;
    }

    @Override
    public long getActiveServerLookupSamplingInterval() {
        return _activeServerLookupSamplingInterval;
    }

    @Override
    public int getThreadPoolSize() {
        return _threadPoolSize;
    }

    @Override
    public SpaceProxyLoadBalancerType getLoadBalancerType() {
        return _loadBalancerType;
    }

    @Override
    public int getNumOfOperationsTypes() {
        return _numOfOperationsTypes;
    }
}

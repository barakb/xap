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

package com.gigaspaces.lrmi;

import com.gigaspaces.internal.lrmi.LRMIMonitoringModule;
import com.j_spaces.kernel.pool.Resource;

/**
 * An encapsulation of a ClientPeer resource
 *
 * @author moran
 * @see ConnectionFactory
 * @see ConnectionPool
 * @since 6.0.2
 */
public abstract class ConnectionResource
        extends Resource
        implements ClientPeer {
    /**
     * Disconnects the resource if it does not belong to the pool.
     */
    @Override
    public void clear() {
        if (!isFromPool() || isClosed())
            this.disconnect();
    }


    public abstract long getGeneratedTraffic();

    public abstract long getReceivedTraffic();

    public abstract void disable();

    protected abstract boolean isClosed();

    public abstract void close();

    public abstract LRMIMonitoringModule getMonitoringModule();

}

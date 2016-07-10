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

package com.gigaspaces.internal.cluster.node.impl.router.spacefinder;

import com.gigaspaces.annotation.lrmi.AsyncRemoteCall;
import com.gigaspaces.internal.cluster.node.impl.router.AbstractReplicationPacket;
import com.gigaspaces.internal.cluster.node.impl.router.IRouterStub;

import java.rmi.RemoteException;

public interface IReplicationConnectionProxy extends IRouterStub {

    <T> T dispatch(AbstractReplicationPacket<T> packet) throws RemoteException;

    @AsyncRemoteCall
    <T> T dispatchAsync(AbstractReplicationPacket<T> packet) throws RemoteException;

    /**
     * @since 8.0.3
     */
    void close() throws RemoteException;
}

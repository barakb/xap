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

package com.gigaspaces.internal.cluster.node.impl;

import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationSourceGroup;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationTargetGroup;
import com.gigaspaces.internal.cluster.node.impl.groups.NoSuchReplicationGroupExistException;
import com.gigaspaces.internal.cluster.node.impl.replica.IReplicaRequestFacade;
import com.gigaspaces.internal.cluster.node.impl.router.ReplicationEndpointDetails;
import com.gigaspaces.internal.cluster.node.impl.router.RouterStubHolder;


public interface IIncomingReplicationFacade extends IReplicaRequestFacade, IReplicationNodePluginFacade {

    IReplicationTargetGroup getReplicationTargetGroup(String groupName) throws NoSuchReplicationGroupExistException;

    IReplicationSourceGroup getReplicationSourceGroup(String groupName) throws NoSuchReplicationGroupExistException;

    /**
     * @since 8.0.5
     */
    RouterStubHolder getRemoteRouterStub(String routerLookupName);

    /**
     * @since 9.0.1
     */
    ReplicationEndpointDetails getEndpointDetails();

}

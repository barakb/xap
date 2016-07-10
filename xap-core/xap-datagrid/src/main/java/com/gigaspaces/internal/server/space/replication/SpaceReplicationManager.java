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

package com.gigaspaces.internal.server.space.replication;

import com.gigaspaces.cluster.replication.ReplicationFilterManager;
import com.gigaspaces.internal.cluster.node.IReplicationNode;
import com.gigaspaces.internal.cluster.node.impl.ReplicationNode;
import com.gigaspaces.internal.cluster.node.impl.router.IReplicationRouter;
import com.gigaspaces.internal.cluster.node.impl.router.ReplicationRouterBuilder;
import com.gigaspaces.internal.sync.mirror.MirrorService;

/**
 * Encapsulates space replication configuration and components.
 *
 * @author Niv Ingberg
 * @since 8.0.4
 */
@com.gigaspaces.api.InternalApi
public class SpaceReplicationManager {
    private final boolean _isReplicated;
    private final boolean _isReplicatedPersistentBlobstore;
    private final boolean _isSyncReplication;
    private final MirrorService _mirrorService;
    private final ReplicationFilterManager _replicationFilterManager;
    private final ReplicationNode _replicationNode;

    public SpaceReplicationManager(SpaceReplicationInitializer replicationInitializer) {
        _isReplicated = replicationInitializer.isReplicated();
        _isReplicatedPersistentBlobstore = replicationInitializer.isReplicatedPersistentBlobstore();
        _isSyncReplication = replicationInitializer.isSyncReplication();
        _mirrorService = replicationInitializer.getMirrorService();
        _replicationFilterManager = replicationInitializer.getReplicationFilterManager();
        _replicationNode = replicationInitializer.getReplicationNode();
    }

    public void close() {
        _replicationNode.close();

        if (_replicationFilterManager != null)
            _replicationFilterManager.close();
    }

    public boolean isReplicated() {
        return _isReplicated;
    }

    public boolean isReplicatedPersistentBlobstore() {
        return _isReplicatedPersistentBlobstore;
    }

    public boolean isMirrorService() {
        return _mirrorService != null;
    }

    public boolean isSyncReplication() {
        return _isSyncReplication;
    }

    public MirrorService getMirrorService() {
        return _mirrorService;
    }

    public ReplicationFilterManager getReplicationFilterManager() {
        return _replicationFilterManager;
    }

    public IReplicationNode getReplicationNode() {
        return _replicationNode;
    }

    public ReplicationRouterBuilder getReplicationRouterBuilder() {
        return _replicationNode.getReplicationRouterBuilder();
    }

    public IReplicationRouter getReplicationRouter() {
        return _replicationNode.getReplicationRouter();
    }
}

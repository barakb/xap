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


package org.openspaces.core.space.filter.replication;

import com.j_spaces.core.cluster.ReplicationFilterProvider;

import org.springframework.beans.factory.InitializingBean;

/**
 * Base class allowing to simplify replicaiton filter provider factories. Requires derived classes
 * to implement {@link #doCreateReplicationFilterProvider()}.
 *
 * @author kimchy
 */
public abstract class AbstractReplicationFilterProviderFactory implements ReplicationFilterProviderFactory, InitializingBean {

    private boolean activeWhenBackup = true;

    private boolean shutdownSpaceOnInitFailure = false;


    private ReplicationFilterProvider replicationFilterProvider;

    public void setActiveWhenBackup(boolean activeWhenBackup) {
        this.activeWhenBackup = activeWhenBackup;
    }

    public void setShutdownSpaceOnInitFailure(boolean shutdownSpaceOnInitFailure) {
        this.shutdownSpaceOnInitFailure = shutdownSpaceOnInitFailure;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        replicationFilterProvider = doCreateReplicationFilterProvider();
        replicationFilterProvider.setActiveWhenBackup(activeWhenBackup);
        replicationFilterProvider.setShutdownSpaceOnInitFailure(shutdownSpaceOnInitFailure);
    }

    /**
     * Creates the replication filter provider. Note, active when backup and shutdown on init flags
     * are set on the {@link com.j_spaces.core.cluster.ReplicationFilterProvider} by this base
     * class.
     */
    protected abstract ReplicationFilterProvider doCreateReplicationFilterProvider();

    @Override
    public ReplicationFilterProvider getFilterProvider() {
        return this.replicationFilterProvider;
    }
}

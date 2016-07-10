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


package com.j_spaces.core.cluster;


/**
 * An {@link com.j_spaces.core.cluster.IReplicationFilter} provider for both input and output
 * replication filteres. Allows, when configuring an embedded Space, to be used to inject actual
 * instances of the replication filters.
 *
 * @author kimchy
 */

public class ReplicationFilterProvider {

    private IReplicationFilter inputFilter;

    private IReplicationFilter outputFilter;

    private boolean activeWhenBackup = true;

    private boolean shutdownSpaceOnInitFailure = false;

    /**
     * Constructs a new replication filter provider with both an input filter and output filter.
     * Note, both filters can have <code>null</code> values indicating no replication filter is
     * perfomed.
     *
     * @param inputFilter  The input replication filter. <code>null</code> value indicates no
     *                     filter.
     * @param outputFilter The output replication filter. <code>null</code> value indicates no
     *                     filter.
     */
    public ReplicationFilterProvider(IReplicationFilter inputFilter, IReplicationFilter outputFilter) {
        this.inputFilter = inputFilter;
        this.outputFilter = outputFilter;
    }

    /**
     * Return the input replication filter. <code>null</code> value indicates no filter.
     */
    public IReplicationFilter getInputFilter() {
        return inputFilter;
    }

    /**
     * Return the output replication filter. <code>null</code> value indicates no filter.
     */
    public IReplicationFilter getOutputFilter() {
        return outputFilter;
    }

    /**
     * Returns <code>true</code> if the filter should be active when the space is in backup mode.
     * Default to <code>true</code>.
     */
    public boolean isActiveWhenBackup() {
        return activeWhenBackup;
    }

    /**
     * <code>true</code> if the filter should be active when the space is in backup mode. Default to
     * <code>true</code>.
     */
    public void setActiveWhenBackup(boolean activeWhenBackup) {
        this.activeWhenBackup = activeWhenBackup;
    }

    /**
     * Returns <code>true</code> if the space should shutdown on filter init failure. Defaults to
     * <code>false</code>.
     */
    public boolean isShutdownSpaceOnInitFailure() {
        return shutdownSpaceOnInitFailure;
    }

    /**
     * <code>true</code> if the space should shutdown on filter init failure. Defaults to
     * <code>false</code>.
     */
    public void setShutdownSpaceOnInitFailure(boolean shutdownSpaceOnInitFailure) {
        this.shutdownSpaceOnInitFailure = shutdownSpaceOnInitFailure;
    }
}

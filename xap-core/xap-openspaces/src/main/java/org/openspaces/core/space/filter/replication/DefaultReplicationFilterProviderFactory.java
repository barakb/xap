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

import com.j_spaces.core.cluster.IReplicationFilter;
import com.j_spaces.core.cluster.ReplicationFilterProvider;

/**
 * The default replication filter provider factory allowing to configure an input and output filter
 * of {@link IReplicationFilter}.
 *
 * @author kimchy
 */
public class DefaultReplicationFilterProviderFactory extends AbstractReplicationFilterProviderFactory {

    private IReplicationFilter inputFilter;

    private IReplicationFilter outputFilter;

    /**
     * Sets the input replication filter.
     */
    public void setInputFilter(IReplicationFilter inputFilter) {
        this.inputFilter = inputFilter;
    }

    /**
     * Sets the output replication filter.
     */
    public void setOutputFilter(IReplicationFilter outputFilter) {
        this.outputFilter = outputFilter;
    }

    @Override
    protected ReplicationFilterProvider doCreateReplicationFilterProvider() {
        if (inputFilter == null && outputFilter == null) {
            throw new IllegalArgumentException("Either input filter or output filter must be set");
        }
        return new ReplicationFilterProvider(inputFilter, outputFilter);
    }
}

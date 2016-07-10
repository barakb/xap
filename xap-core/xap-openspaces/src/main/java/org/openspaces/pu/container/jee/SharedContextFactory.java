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


package org.openspaces.pu.container.jee;

import org.openspaces.core.cluster.ClusterInfo;
import org.openspaces.core.cluster.ClusterInfoAware;
import org.springframework.beans.factory.FactoryBean;

/**
 * A helper to construct web context path. Allows to set the {@link #setContext(String) context}
 * that will be used.
 *
 * <p>{@link #setUnique(boolean)} (which defaults to <code>true</code>) controls if the context
 * created will be unique between other instances of the same web application. If set to
 * <code>true</code>, the context created will be in the format of <code>[context] + [suffix] +
 * {@link org.openspaces.core.cluster.ClusterInfo#getRunningNumberOffset1()}</code>. If set to
 * <code>false</code>, will simply return the context.
 *
 * @author kimchy
 */
public class SharedContextFactory implements FactoryBean, ClusterInfoAware {

    private String context;

    private boolean unique = true;

    private String separator = "_";

    private ClusterInfo clusterInfo;

    public void setContext(String context) {
        this.context = context;
    }

    public void setUnique(boolean unique) {
        this.unique = unique;
    }

    public void setSeparator(String separator) {
        this.separator = separator;
    }

    public void setClusterInfo(ClusterInfo clusterInfo) {
        this.clusterInfo = clusterInfo;
    }

    public Object getObject() throws Exception {
        if (!unique) {
            return context;
        }
        return context + separator + clusterInfo.getRunningNumberOffset1();
    }

    public Class getObjectType() {
        return String.class;
    }

    public boolean isSingleton() {
        return true;
    }
}

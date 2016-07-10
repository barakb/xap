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


package org.openspaces.events.config;

import org.openspaces.events.adapter.AbstractResultEventListenerAdapter;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;

/**
 * @author kimchy
 */
public abstract class AbstractResultEventAdapterFactoryBean implements FactoryBean, InitializingBean {

    private Long writeLease;

    private Boolean updateOrWrite;

    private Long updateTimeout;

    private AbstractResultEventListenerAdapter adapter;

    public void setWriteLease(Long writeLease) {
        this.writeLease = writeLease;
    }

    public void setUpdateOrWrite(Boolean updateOrWrite) {
        this.updateOrWrite = updateOrWrite;
    }

    public void setUpdateTimeout(Long updateTimeout) {
        this.updateTimeout = updateTimeout;
    }

    public void afterPropertiesSet() throws Exception {
        adapter = createAdapter();
        if (writeLease != null) {
            adapter.setWriteLease(writeLease);
        }
        if (updateOrWrite != null) {
            adapter.setUpdateOrWrite(updateOrWrite);
        }
        if (updateTimeout != null) {
            adapter.setUpdateTimeout(updateTimeout);
        }
        if (adapter instanceof InitializingBean) {
            ((InitializingBean) adapter).afterPropertiesSet();
        }
    }

    protected abstract AbstractResultEventListenerAdapter createAdapter();

    public Object getObject() throws Exception {
        return this.adapter;
    }

    public Class<?> getObjectType() {
        return adapter == null ? AbstractResultEventListenerAdapter.class : adapter.getClass();
    }

    public boolean isSingleton() {
        return true;
    }
}

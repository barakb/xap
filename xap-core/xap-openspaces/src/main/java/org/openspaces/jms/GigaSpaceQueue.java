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


package org.openspaces.jms;

import com.gigaspaces.internal.jms.JmsFactory;

import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

/**
 * A Spring factory bean to create {@link javax.jms.Queue} based on a queue name.
 *
 * @author kimchy
 */
public class GigaSpaceQueue implements FactoryBean, InitializingBean {

    private String queueName;

    private Object queue;

    /**
     * The queue name for this JMS queue.
     */
    public void setName(String queueName) {
        this.queueName = queueName;
    }

    public void afterPropertiesSet() throws Exception {
        Assert.notNull(queueName, "queueName property is required");
        queue = JmsFactory.getInstance().createQueue(queueName);
    }

    public Object getObject() throws Exception {
        return queue;
    }

    public Class getObjectType() {
        return queue == null ? JmsFactory.getInstance().getQueueClass() : queue.getClass();
    }

    public boolean isSingleton() {
        return true;
    }
}

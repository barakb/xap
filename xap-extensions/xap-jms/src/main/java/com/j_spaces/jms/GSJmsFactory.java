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

package com.j_spaces.jms;

import com.gigaspaces.internal.jms.JmsFactory;
import com.j_spaces.core.IJSpace;

import java.io.Closeable;

import javax.naming.Context;

/**
 * @author Niv Ingberg
 * @since 10.1
 */
public class GSJmsFactory extends JmsFactory {

    public Object createConnectionFactory(IJSpace spaceProxy) throws Exception {
        return new GSConnectionFactoryImpl(spaceProxy, null);
    }

    public Object createTopicConnectionFactory(IJSpace spaceProxy) throws Exception {
        return new GSConnectionFactoryImpl(spaceProxy, null);
    }

    public Object createQueueConnectionFactory(IJSpace spaceProxy) throws Exception {
        return new GSConnectionFactoryImpl(spaceProxy, null);
    }

    public Object createXATopicConnectionFactory(IJSpace spaceProxy) throws Exception {
        return new GSXATopicConnectionFactoryImpl(spaceProxy, null);
    }

    public Object createXAQueueConnectionFactory(IJSpace spaceProxy) throws Exception {
        return new GSXAQueueConnectionFactoryImpl(spaceProxy, null);
    }

    @Override
    public Class getTopicClass() {
        return GSTopicImpl.class;
    }

    @Override
    public Object createTopic(String name) {
        return new GSTopicImpl(name);
    }

    @Override
    public Class getQueueClass() {
        return GSQueueImpl.class;
    }

    @Override
    public Object createQueue(String name) {
        return new GSQueueImpl(name);
    }

    @Override
    public Closeable createLookupManager(String containerName, IJSpace spaceProxy, Context internalContext, Context externalContext) throws Exception {
        GsJmsLookupManager result = new GsJmsLookupManager(containerName, spaceProxy, this, internalContext, externalContext);
        result.initialize();
        return result;
    }
}

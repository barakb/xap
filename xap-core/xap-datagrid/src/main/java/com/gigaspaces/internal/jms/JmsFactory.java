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

package com.gigaspaces.internal.jms;

import com.j_spaces.core.IJSpace;
import com.j_spaces.kernel.ClassLoaderHelper;
import com.j_spaces.kernel.SystemProperties;

import java.io.Closeable;

import javax.naming.Context;

/**
 * @author Niv Ingberg
 * @since 10.1
 */
public abstract class JmsFactory {

    private static JmsFactory instance;

    public static JmsFactory getInstance() {
        if (instance == null) {
            final String className = System.getProperty(SystemProperties.JMS_FACTORY, "com.j_spaces.jms.GSJmsFactory");
            try {
                instance = ClassLoaderHelper.newInstance(className);
            } catch (Exception e) {
                throw new IllegalArgumentException("Failed to create JmsFactory using '" + className + "'");
            }
        }
        return instance;
    }

    public abstract Class getTopicClass();

    public abstract Object createTopic(String name);

    public abstract Class getQueueClass();

    public abstract Object createQueue(String name);

    public abstract Closeable createLookupManager(String containerName, IJSpace spaceProxy,
                                                  Context internalContext, Context externalContext) throws Exception;
}

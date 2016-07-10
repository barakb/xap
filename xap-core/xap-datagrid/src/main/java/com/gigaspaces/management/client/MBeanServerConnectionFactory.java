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

package com.gigaspaces.management.client;

import com.gigaspaces.internal.jmx.JMXUtilities;

import javax.management.MBeanServerConnection;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

/**
 * @author evgenyf
 */
@com.gigaspaces.api.InternalApi
public class MBeanServerConnectionFactory {

    /**
     * create MBean server connection according to certain JNDI URL
     *
     * @return MBean server connection
     */
    public static MBeanServerConnection createMBeanServerConnection(String jndiURL)
            throws Exception {
        JMXServiceURL url = new JMXServiceURL(JMXUtilities.createJMXUrl(jndiURL));

        JMXConnector jmxc = JMXConnectorFactory.connect(url, null);
        return jmxc.getMBeanServerConnection();
    }
}

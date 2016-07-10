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

import org.openspaces.pu.service.PlainServiceDetails;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * A service that holds a jee container (such as jetty).
 *
 * @author kimchy
 */
public class JeeServiceDetails extends PlainServiceDetails {

    private static final long serialVersionUID = 5347342326588251565L;

    public static final String ID = "jee-container";
    public static final String SERVICE_TYPE = "jee-container";

    public static final class Attributes {
        public static final String HOST = "host";
        public static final String PORT = "port";
        public static final String SSLPORT = "ssl-port";
        public static final String CONTEXTPATH = "context-path";
        public static final String SHARED = "shared";
        public static final String JEETYPE = "jee-type";
    }

    public JeeServiceDetails() {
    }

    public JeeServiceDetails(String host, int port, int sslPort, String contextPath, boolean shared,
                             String serviceSubType, JeeType jeeType) {
        super(ID, SERVICE_TYPE, serviceSubType, host + ":" + port + contextPath, host + ":" + port + contextPath);
        getAttributes().put(Attributes.HOST, host);
        getAttributes().put(Attributes.PORT, port);
        getAttributes().put(Attributes.SSLPORT, sslPort);
        getAttributes().put(Attributes.CONTEXTPATH, contextPath);
        getAttributes().put(Attributes.SHARED, shared);
        getAttributes().put(Attributes.JEETYPE, jeeType);
    }

    /**
     * Returns the host of where the service is running on.
     */
    public String getHost() {
        return (String) getAttributes().get(Attributes.HOST);
    }

    /**
     * Returns the port of where the service is running on.
     */
    public Integer getPort() {
        return (Integer) getAttributes().get(Attributes.PORT);
    }

    /**
     * Returns the ssl port of where the service is running on.
     */
    public Integer getSslPort() {
        return (Integer) getAttributes().get(Attributes.SSLPORT);
    }

    /**
     * Returns the context path of the web application.
     */
    public String getContextPath() {
        return (String) getAttributes().get(Attributes.CONTEXTPATH);
    }

    /**
     * Returns <code>true</code> if this web service is running on a shared instance of a web
     * container. <code>false</code> if the web application instance is running on its own dedicated
     * web container.
     */
    public Boolean isShared() {
        return (Boolean) getAttributes().get(Attributes.SHARED);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
    }
}

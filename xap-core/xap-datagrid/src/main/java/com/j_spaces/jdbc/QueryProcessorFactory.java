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

package com.j_spaces.jdbc;

import com.gigaspaces.internal.client.spaceproxy.ISpaceProxy;
import com.gigaspaces.internal.server.space.IRemoteSpace;
import com.gigaspaces.security.directory.CredentialsProvider;
import com.gigaspaces.security.service.SecurityInterceptor;
import com.j_spaces.core.IJSpace;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * Factory for QueryProcessor that creates the appropriate instance according to configuration
 *
 * @author anna
 * @since 6.1
 */
@com.gigaspaces.api.InternalApi
public class QueryProcessorFactory {
    public static final String COM_GS_EMBEDDED_QP_ENABLED = "com.gs.embeddedQP.enabled";
    public static final String COM_GIGASPACES_EMBEDDED_QP_ENABLED = "com.gigaspaces.embeddedQP.enabled";

    // System property that indicates that the QP runs embedded
    private static final String COM_GS_EMBEDDED_QP_PROPERTIES = "com.gs.embeddedQP.properties";
    private static final String COM_GIGASPACES_EMBEDDED_QP_PROPERTIES = "com.gigaspaces.embeddedQP.properties";

    /**
     * Creates local or remote QP according to configuration
     */
    public static IQueryProcessor newInstance(IJSpace proxy, IRemoteSpace remoteSpace, Properties config)
            throws Exception {
        if (isRemoteQueryProcessor(config))
            return remoteSpace.getQueryProcessor();

        ISpaceProxy clusteredProxy = (ISpaceProxy) proxy;
        ISpaceProxy singleProxy = clusteredProxy;
        if (clusteredProxy.isClustered()) {
            singleProxy = (ISpaceProxy) clusteredProxy.getDirectProxy().getNonClusteredProxy();
            CredentialsProvider credentialsProvider = clusteredProxy.getDirectProxy().getSecurityManager()
                    .getCredentialsProvider();
            if (credentialsProvider != null)
                singleProxy.getDirectProxy().getSecurityManager().login(credentialsProvider);
        }
        return newLocalInstance(clusteredProxy, singleProxy, config, null);
    }

    /**
     * Create new QueryProcessor instance using the connection properties and the qp.properties
     */
    public static IQueryProcessor newLocalInstance(IJSpace clusterProxy, IJSpace spaceProxy,
                                                   Properties connectionProperties, SecurityInterceptor securityInterceptor) throws Exception {
        Properties embeddedProperties = loadEmbeddedPropertiesFile();
        Properties mergedProps = mergeProperties(connectionProperties, embeddedProperties);
        return new QueryProcessor(clusterProxy, spaceProxy, mergedProps, securityInterceptor);
    }

    /**
     * Merges two properties files.
     */
    private static Properties mergeProperties(Properties overrideProperties, Properties properties) {
        Properties merged = new Properties();

        if (properties != null)
            merged.putAll(properties);

        if (overrideProperties != null)
            merged.putAll(overrideProperties);

        return merged;
    }

    private static Properties loadEmbeddedPropertiesFile() throws IOException {
        String propsFile = System.getProperty(COM_GIGASPACES_EMBEDDED_QP_PROPERTIES);
        if (propsFile == null)
            propsFile = System.getProperty(COM_GS_EMBEDDED_QP_PROPERTIES);

        Properties info = new Properties();

        if (propsFile == null)
            return info;

        FileInputStream fileIn = new FileInputStream(propsFile);
        info.load(fileIn);
        fileIn.close();
        return info;
    }

    private static boolean isRemoteQueryProcessor(Properties config) {
        boolean embeddedQP = false;
        // Read embedded QP configuration from connection properties
        String isLocalValue = config.getProperty(COM_GIGASPACES_EMBEDDED_QP_ENABLED);
        if (isLocalValue == null)
            isLocalValue = config.getProperty(COM_GS_EMBEDDED_QP_ENABLED);
        // If not configured through connection properties get from system properties
        if (isLocalValue == null) {
            isLocalValue = System.getProperty(COM_GIGASPACES_EMBEDDED_QP_ENABLED);
            if (isLocalValue == null)
                isLocalValue = System.getProperty(COM_GS_EMBEDDED_QP_ENABLED);
        }
        if (isLocalValue != null && isLocalValue.equalsIgnoreCase("true"))
            embeddedQP = true;
        return !embeddedQP;
    }
}

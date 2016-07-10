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

package com.gigaspaces.lrmi;

import com.gigaspaces.config.ConfigurationException;
import com.gigaspaces.config.lrmi.ITransportConfig;
import com.j_spaces.kernel.ClassLoaderHelper;

import java.util.HashMap;

/**
 * The ProtocolRegistry maps between LRMI Protocol names to Protocol Adapters.
 *
 * Each Protocol Adapter in an LRMI environment must have a unique name.
 *
 * A Protocol Adapter must be registered in the Protocol Registry before it can be used by an
 * application. The Protocol Registry initializes some protocols provided as arguments to init()
 * with appropriate side {@link ProtocolAdapter.Side}.
 *
 * @author Igor Goldenberg
 * @since 4.0
 */
@com.gigaspaces.api.InternalApi
public class ProtocolRegistry
        extends HashMap<String, ProtocolAdapter<?>> {
    private static final long serialVersionUID = 1L;

    /**
     * Initializes this registry with protocol adapters defined in the supplied properties.
     */
    synchronized public void init(ITransportConfig config, ProtocolAdapter.Side initSide)
            throws ConfigurationException {
        String adapterName = config.getProtocolName();
        String adapterClass = config.getProtocolAdaptorClass();

        try {
            ProtocolAdapter adapter = get(adapterName);
            if (adapter == null) {
                adapter = (ProtocolAdapter) ClassLoaderHelper.loadClass(adapterClass).newInstance();
                adapter.init(config, initSide);
                put(adapterName, adapter);
            } else
                adapter.init(config, initSide);
        } catch (Exception ex) {
            throw new ConfigurationException("Failed to init. Protocol Registry: " + adapterClass, ex);
        }
    }

    public int getPort(ITransportConfig config) {
        return get(config.getProtocolName()).getPort();
    }

    public LRMIMonitoringDetails getMonitoringDetails(ITransportConfig config) {
        return get(config.getProtocolName()).getMonitoringDetails();
    }
}
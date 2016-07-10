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

import com.gigaspaces.config.lrmi.ITransportConfig;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.j_spaces.kernel.pool.IResourceFactory;

@com.gigaspaces.api.InternalApi
public class ConnectionFactory
        implements IResourceFactory<ConnectionResource> {
    final private ProtocolAdapter<ConnectionResource> protocolAdapter;
    final private ITransportConfig config;
    final private PlatformLogicalVersion serviceVersion;

    ConnectionFactory(ProtocolAdapter<ConnectionResource> protocolAdapter, ITransportConfig config, PlatformLogicalVersion serviceVersion) {
        this.protocolAdapter = protocolAdapter;
        this.config = config;
        this.serviceVersion = serviceVersion;
    }

    public ConnectionResource allocate() {
        ConnectionResource conn = protocolAdapter.getClientPeer(serviceVersion);
        conn.init(config);

        return conn;
    }

}

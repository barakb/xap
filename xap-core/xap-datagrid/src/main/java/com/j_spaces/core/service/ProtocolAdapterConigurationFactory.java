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

package com.j_spaces.core.service;

import com.gigaspaces.config.lrmi.ITransportConfig;
import com.gigaspaces.config.lrmi.nio.NIOConfiguration;

import java.net.UnknownHostException;

@com.gigaspaces.api.InternalApi
public class ProtocolAdapterConigurationFactory {
    private static final String PADAPTER_PROTOCOL_CONFIGURATION = "lrmi";

    public static ITransportConfig create() throws UnknownHostException {
        String protocolName = System.getProperty("com.gs.transport_protocol", PADAPTER_PROTOCOL_CONFIGURATION);
        if (protocolName.equals(PADAPTER_PROTOCOL_CONFIGURATION))
            return NIOConfiguration.create();

        throw new IllegalStateException("Unknown transport protocol (" + protocolName + ")");
    }

}

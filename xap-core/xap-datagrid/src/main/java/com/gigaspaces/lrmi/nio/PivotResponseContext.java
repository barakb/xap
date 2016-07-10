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

package com.gigaspaces.lrmi.nio;

import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.gigaspaces.lrmi.LRMIInvocationTrace;
import com.gigaspaces.lrmi.OperationPriority;
import com.gigaspaces.lrmi.ProtocolAdapter;

/**
 * {@link Pivot} specific response context used by {@link PAdapter} implementation of {@link
 * ProtocolAdapter}
 *
 * @author eitany
 * @since 7.0
 */
@com.gigaspaces.api.InternalApi
public class PivotResponseContext extends AbstractResponseContext {
    private final Pivot _pivot;
    private final ChannelEntry _channel;

    public PivotResponseContext(Pivot pivot, ChannelEntry channel, IResponseHandler handler,
                                PlatformLogicalVersion sourcePlatformLogicalVersion, OperationPriority operationPriority,
                                String lrmiMonitoringId, LRMIInvocationTrace trace) {
        super(sourcePlatformLogicalVersion, operationPriority, lrmiMonitoringId, trace);
        this._channel = channel;
        this._pivot = pivot;
        setResponseHandler(handler);
    }

    @Override
    public void sendResponseToClient(ReplyPacket<?> respPacket) {
        _pivot.requestPending(_channel, respPacket, this);
    }
}

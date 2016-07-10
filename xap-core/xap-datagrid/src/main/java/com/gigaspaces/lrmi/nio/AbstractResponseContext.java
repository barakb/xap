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
import com.gigaspaces.lrmi.LRMIInvocationContext;
import com.gigaspaces.lrmi.LRMIInvocationContext.InvocationStage;
import com.gigaspaces.lrmi.LRMIInvocationContext.ProxyWriteType;
import com.gigaspaces.lrmi.LRMIInvocationTrace;
import com.gigaspaces.lrmi.OperationPriority;

/**
 * Base class for response context implementations
 *
 * @author eitany
 * @since 7.0
 */
public abstract class AbstractResponseContext implements IResponseContext {
    private boolean _sendResponse = true;
    private boolean _responseSent = false;
    private IResponseHandler _respHandler;
    private final PlatformLogicalVersion _sourcePlatformLogicalVersion;
    private final LRMIInvocationTrace _trace;
    private final OperationPriority _operationPriority;
    private final String _lrmiMonitoringId;

    private boolean _invokedFromNewRouter;

    public AbstractResponseContext(PlatformLogicalVersion sourcePlatformLogicalVersion,
                                   OperationPriority operationPriority, String lrmiMonitoringId, LRMIInvocationTrace trace) {
        _sourcePlatformLogicalVersion = sourcePlatformLogicalVersion;
        _operationPriority = operationPriority;
        _lrmiMonitoringId = lrmiMonitoringId;
        _trace = trace;
    }

    @Override
    public synchronized boolean markAsSentResponse() {
        return _responseSent ? false : (_responseSent = true);
    }

    @Override
    public void sendResponse(Object response, Exception exp) {
        try {
            //TODO: Do we really need a new snapshot here?
            LRMIInvocationContext.updateContext(_trace, ProxyWriteType.UNCACHED, InvocationStage.SERVER_MARSHAL_REPLY, _sourcePlatformLogicalVersion, null, true, null, null);
            ReplyPacket<?> respPacket = new ReplyPacket<Object>(response, exp);
            _respHandler.handleResponse(this, respPacket);
        } finally {
            LRMIInvocationContext.restoreContext();
        }
    }

    @Override
    public void setResponseHandler(IResponseHandler handler) {
        _respHandler = handler;
    }

    @Override
    public void setSendResponse(boolean sendResp) {
        _sendResponse = sendResp;
    }

    @Override
    public boolean shouldSendResponse() {
        return _sendResponse;
    }

    @Override
    public PlatformLogicalVersion getSourcePlatformLogicalVersion() {
        return _sourcePlatformLogicalVersion;
    }

    @Override
    public OperationPriority getOperationPriority() {
        return _operationPriority;
    }

    @Override
    public LRMIInvocationTrace getTrace() {
        return _trace;
    }

    @Override
    public boolean isInvokedFromNewRouter() {
        return _invokedFromNewRouter;
    }

    public void setInvokedFromNewRouter(boolean invokedFromNewRouter) {
        _invokedFromNewRouter = invokedFromNewRouter;
    }

    @Override
    public String getLRMIMonitoringId() {
        return _lrmiMonitoringId;
    }
}
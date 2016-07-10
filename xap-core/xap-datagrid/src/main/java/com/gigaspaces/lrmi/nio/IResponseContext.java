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

/**
 * Represent a response context that is passed by thread local using {@link ResponseContext} for
 * callback invocations
 *
 * @author eitany
 * @since 7.0
 */
public interface IResponseContext {
    void setSendResponse(boolean sendResp);

    void setResponseHandler(IResponseHandler handler);

    /**
     * Mark this context as response was already sent. This method is synchronized and not using CAS
     * since no race is expected and such race can only happen on bug.
     *
     * @return <code>false</code> if was already marked.
     */
    boolean markAsSentResponse();

    void sendResponse(Object response, Exception exp);

    void sendResponseToClient(ReplyPacket<?> respPacket);

    boolean shouldSendResponse();

    PlatformLogicalVersion getSourcePlatformLogicalVersion();

    LRMIInvocationTrace getTrace();

    OperationPriority getOperationPriority();

    boolean isInvokedFromNewRouter();

    void setInvokedFromNewRouter(boolean invokedFromNewRouter);

    String getLRMIMonitoringId();
}
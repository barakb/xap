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


/**
 * This is a thread local based context that is used to send response back to the client not in the
 * current stack, but rather from a different thread that will use a stored context in the memory.
 *
 * it is being used to avoid blocking the LMRI thread in blocking operations and letting the
 * processor (matching) thread send the response when available.
 *
 * @author asy ronen
 * @since 6.1
 */
@com.gigaspaces.api.InternalApi
public class ResponseContext {
    final private static ThreadLocal<IResponseContext> _threadLocal = new ThreadLocal<IResponseContext>();

    public static IResponseContext getResponseContext() {
        return _threadLocal.get();
    }

    public static void clearResponseContext() {
        _threadLocal.remove();
    }

    public static boolean isCallBackMode() {
        return _threadLocal.get() != null;
    }

    public static void dontSendResponse() {
        getResponseContext().setSendResponse(false);
    }

    public static void setExistingResponseContext(IResponseContext respContext) {
        respContext.setSendResponse(true);
        _threadLocal.set(respContext);
    }
}

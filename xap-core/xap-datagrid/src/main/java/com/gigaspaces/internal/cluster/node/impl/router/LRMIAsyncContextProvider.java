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

package com.gigaspaces.internal.cluster.node.impl.router;

import com.gigaspaces.async.AsyncFuture;
import com.gigaspaces.lrmi.LRMIUtilities;
import com.gigaspaces.lrmi.nio.async.FutureContext;
import com.gigaspaces.lrmi.nio.async.IExceptionHandler;
import com.gigaspaces.lrmi.nio.async.IFuture;
import com.gigaspaces.lrmi.nio.async.LRMIFuture;

@com.gigaspaces.api.InternalApi
public class LRMIAsyncContextProvider
        implements IAsyncContextProvider {

    @Override
    public void setExceptionHandler(IExceptionHandler exceptionHandler) {
        FutureContext.setExceptionHandler(exceptionHandler);
    }

    public <T> AsyncFuture<T> getFutureContext(T syncResult, Object proxy) {
        if (LRMIUtilities.isRemoteProxy(proxy)) {
            IFuture futureResult = FutureContext.getFutureResult();
            FutureContext.clear();
            return futureResult;
        } else {
            LRMIFuture<T> future = new LRMIFuture<T>(true, Thread.currentThread().getContextClassLoader());
            future.setResult(syncResult);
            return future;
        }
    }

}

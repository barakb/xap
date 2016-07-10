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

package com.gigaspaces.lrmi.nio.async;

import com.gigaspaces.async.AsyncFutureListener;

/**
 * @author AssafR
 * @since 6.6
 */
@com.gigaspaces.api.InternalApi
public class FutureContext {
    private static final ThreadLocal<FutureParams> _context = new ThreadLocal<FutureParams>();

    public static class FutureParams {

        private FutureParams() {
        }

        private AsyncFutureListener listener;
        private IResultTransformer transformator;
        private IExceptionHandler exceptionHandler;

        private IFuture result;

        public AsyncFutureListener getListener() {
            return listener;
        }

        public IResultTransformer getTransformator() {
            return transformator;
        }

        public IExceptionHandler getExceptionHandler() {
            return exceptionHandler;
        }

        public IFuture getResult() {
            return result;
        }
    }

    public static void clear() {
        _context.set(null);
    }

    public static void setFutureListener(AsyncFutureListener listener) {
        FutureParams futureParams = _context.get();
        if (futureParams == null) {
            futureParams = new FutureParams();
            _context.set(futureParams);
        }

        futureParams.listener = listener;
    }

    public static void setResultTransformator(IResultTransformer transformator) {
        FutureParams futureParams = _context.get();
        if (futureParams == null) {
            futureParams = new FutureParams();
            _context.set(futureParams);
        }

        futureParams.transformator = transformator;
    }

    public static void setExceptionHandler(IExceptionHandler exceptionHandler) {
        FutureParams futureParams = _context.get();
        if (futureParams == null) {
            futureParams = new FutureParams();
            _context.set(futureParams);
        }

        futureParams.exceptionHandler = exceptionHandler;
    }

    public static void setFutureResult(IFuture result) {
        FutureParams futureParams = _context.get();
        if (futureParams == null) {
            futureParams = new FutureParams();
            _context.set(futureParams);
        }

        futureParams.result = result;
    }

    public static IFuture getFutureResult() {
        FutureParams ctx = _context.get();
        if (ctx == null)
            return null;

        return ctx.result;
    }

    public static AsyncFutureListener getFutureListener() {
        FutureParams ctx = _context.get();
        if (ctx == null)
            return null;

        return ctx.listener;
    }

    public static IResultTransformer getResultTransformer() {
        FutureParams ctx = _context.get();
        if (ctx == null)
            return null;

        return ctx.transformator;
    }

    public static IExceptionHandler getExceptionhandler() {
        FutureParams ctx = _context.get();
        if (ctx == null)
            return null;

        return ctx.exceptionHandler;
    }

    public static FutureParams getFutureParams() {
        return _context.get();
    }
}

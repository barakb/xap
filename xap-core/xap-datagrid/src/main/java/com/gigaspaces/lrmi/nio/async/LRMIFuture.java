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
import com.gigaspaces.async.AsyncResult;
import com.gigaspaces.async.internal.DefaultAsyncResult;
import com.gigaspaces.exception.lrmi.ApplicationException;
import com.gigaspaces.exception.lrmi.ProtocolException;
import com.gigaspaces.internal.utils.concurrent.ContextClassLoaderRunnable;
import com.gigaspaces.lrmi.LRMIRuntime;
import com.gigaspaces.lrmi.nio.ReplyPacket;
import com.gigaspaces.lrmi.nio.async.FutureContext.FutureParams;
import com.j_spaces.kernel.ClassLoaderHelper;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A future implementation used to hold delayed result coming from one target. A callback can be
 * used to signal higher level logic (composite future)
 *
 * @author asy ronen.
 * @since 6.5
 */
@com.gigaspaces.api.InternalApi
public class LRMIFuture<T> implements IFuture<T> {
    private volatile boolean canceled = false;
    private volatile ReplyPacket<T> replyPacket = null;

    final private Lock _lock = new ReentrantLock();
    final private Condition _resultCondition = _lock.newCondition();

    private volatile AsyncFutureListener<T> listener = null;
    final private IResultTransformer<T> transformer;
    final private IExceptionHandler exceptionHandler;

    private final AtomicBoolean triggeredEvent = new AtomicBoolean();
    private final boolean embedded;
    private volatile ClassLoader contextClassLoader;

    public LRMIFuture(boolean embedded, ClassLoader contextClassLoader) {
        this.embedded = embedded;
        this.contextClassLoader = contextClassLoader;
        FutureParams futureParams = FutureContext.getFutureParams();
        if (futureParams != null) {
            this.transformer = futureParams.getTransformator();
            this.exceptionHandler = futureParams.getExceptionHandler();
            this.listener = futureParams.getListener();
        } else {
            this.transformer = null;
            this.exceptionHandler = null;
            this.listener = null;
        }
        FutureContext.clear();
    }

    public LRMIFuture(ClassLoader contextClassLoader) {
        this(false, contextClassLoader);
    }

    public void setListener(AsyncFutureListener<T> listener) {
        this.listener = listener;
        if (isDone()) {
            LRMIRuntime.getRuntime().getThreadPool().execute(new ContextClassLoaderRunnable(contextClassLoader) {
                @Override
                protected void execute() {
                    sendEvent();
                }
            });
        }
    }

    public boolean cancel(boolean mayInterruptIfRunning) {
        canceled = true;
        if (mayInterruptIfRunning && !isDone()) {
            try {
                _lock.lock();

                _resultCondition.signalAll();
            } finally {
                _lock.unlock();
            }
        }
        return true;
    }

    public boolean isCancelled() {
        return canceled;
    }

    public boolean isDone() {
        return replyPacket != null;
    }

    public T get() throws InterruptedException, ExecutionException {
        try {
            _lock.lock();

            checkState(); // check for canceled

            if (isDone())
                return getResult();

            _resultCondition.await();

            checkState(); // check for canceled 

            return getResult();
        } finally {
            _lock.unlock();
        }
    }

    public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        try {
            _lock.lock();

            checkState(); // check for canceled

            if (isDone())
                return getResult();

            if (!_resultCondition.await(timeout, unit))
                throw new TimeoutException("Timeout waiting for result for [" + timeout + "]");

            checkState(); // check for canceled

            return getResult();
        } finally {
            _lock.unlock();
        }
    }

    protected T getResult() throws ExecutionException {
        if (replyPacket.getException() != null)
            throw new ExecutionException(replyPacket.getException());

        T result = replyPacket.getResult();
        if (transformer != null) {
            result = transformer.transform(result);
        }

        return result;
    }

    public void reset(ClassLoader contextClassLoader) {
        this.contextClassLoader = contextClassLoader;
        replyPacket = null;
        triggeredEvent.set(false);
    }

    public void setResultPacket(final ReplyPacket<T> replyPacket) {
        if (!needsSpawning(replyPacket)) {
            //TODO ugly, should be somehow unneeded, maybe having the selector thread writing using
            //the write context class loader
            //Change context class loader if needed
            final ClassLoader currentClassLoader = Thread.currentThread().getContextClassLoader();
            final boolean changeClassLoader = currentClassLoader != contextClassLoader;
            if (changeClassLoader)
                ClassLoaderHelper.setContextClassLoader(contextClassLoader, true /*ignore security*/);
            try {
                setResultPacketInternal(replyPacket);
            } finally {
                if (changeClassLoader)
                    ClassLoaderHelper.setContextClassLoader(currentClassLoader, true /*ignore security*/);
            }
        } else {
            LRMIRuntime.getRuntime().getThreadPool().execute(new ContextClassLoaderRunnable(contextClassLoader) {
                @Override
                protected void execute() {
                    setResultPacketInternal(replyPacket);
                }
            });
        }
    }

    private boolean needsSpawning(ReplyPacket<T> replyPacket) {
        return embedded && (replyPacket.getException() != null || listener != null);
    }

    private void setResultPacketInternal(ReplyPacket<T> replyPacket) {
        Throwable newException;
        if (replyPacket.getException() != null) {
            newException = replyPacket.getException();
            while (newException instanceof ExecutionException || newException instanceof ProtocolException || newException instanceof ApplicationException) {
                newException = newException.getCause();
            }
            if (exceptionHandler != null) {
                newException = exceptionHandler.handleException(newException, this);
                if (newException == null) {
                    return;
                }

            }
            replyPacket.setException(newException instanceof Exception ? (Exception) newException : new ExecutionException(newException));
        }

        this.replyPacket = replyPacket;

        try {
            _lock.lock();

            _resultCondition.signalAll();
        } finally {
            _lock.unlock();

        }

        sendEvent();
    }

    private void sendEvent() {
        if (listener != null && triggeredEvent.compareAndSet(false, true)) {
            AsyncResult<T> res;
            if (replyPacket.getException() != null) {
                res = new DefaultAsyncResult<T>(null, replyPacket.getException());
            } else {
                T result = replyPacket.getResult();
                Exception exp = null;
                if (transformer != null) {
                    try {
                        result = transformer.transform(result);
                    } catch (ExecutionException e) {
                        exp = e;
                    }
                }

                res = new DefaultAsyncResult<T>(result, exp);
            }

            listener.onResult(res);
        }
    }

    public void setResult(Object result) {
        if (result instanceof Exception) {
            setResultPacket(new ReplyPacket<T>(null, (Exception) result));
        } else {
            setResultPacket(new ReplyPacket<T>((T) result, null));
        }
    }

    /**
     * Throws CancellationException if the future was canceled.
     */
    private void checkState() {
        if (isCancelled())
            throw new CancellationException("task was cancelled");
    }
}


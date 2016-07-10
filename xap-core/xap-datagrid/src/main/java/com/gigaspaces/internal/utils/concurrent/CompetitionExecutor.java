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

package com.gigaspaces.internal.utils.concurrent;

import com.gigaspaces.internal.utils.concurrent.IAsyncHandlerProvider.CycleResult;
import com.j_spaces.core.exception.ClosedResourceException;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * @author Niv Ingberg
 * @since 9.0.1
 */
public abstract class CompetitionExecutor<T extends CompetitiveTask> implements IAsyncHandlerProviderStateListener {
    private final IAsyncHandlerProvider _asyncHandlerProvider;
    private final ExchangeCountDownLatch<T> _completionLatch;
    private final Object _lock;
    private int _pendingCompetitors;
    private volatile Throwable _error;

    protected CompetitionExecutor(IAsyncHandlerProvider asyncHandlerProvider) {
        this._asyncHandlerProvider = asyncHandlerProvider;
        this._completionLatch = new ExchangeCountDownLatch<T>();
        this._lock = new Object();
    }

    protected void start(T[] competitors, long idleDelay, String name) {
        //Flush to main memory
        synchronized (_lock) {
            this._pendingCompetitors = competitors.length;
        }

        for (int i = 0; i < competitors.length; i++)
            _asyncHandlerProvider.start(wrapCompetitor(competitors[i]), idleDelay, name + "-competitor_" + i + "_of_" + competitors.length, false);

        if (!_asyncHandlerProvider.addStateListener(this))
            onClosed();
    }

    protected abstract CompetitorWrapper wrapCompetitor(T competitor);

    @Override
    public void onClosed() {
        onCompletion(null, new ClosedResourceException("Underlying resources are being closed"));
    }

    public boolean isCompleted() {
        return _completionLatch.getCount() == 0;
    }

    public T await(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException {
        if (!_completionLatch.await(timeout, unit)) {
            close();
            return null;
        }

        if (_error != null)
            throw new ExecutionException(_error);
        return _completionLatch.get();
    }

    private void close() {
        _asyncHandlerProvider.removeStateListener(this);
    }

    private void onCompletion(T competitor, Throwable error) {
        boolean isCompleted = false;
        synchronized (_lock) {
            if (isCompleted())
                return;

            if (competitor != null || error != null || --_pendingCompetitors <= 0) {
                _error = error;
                _completionLatch.countDown(competitor);
                isCompleted = true;
            }
        }

        if (isCompleted)
            close();
    }

    protected abstract class CompetitorWrapper extends AsyncCallable {
        private final T _competitor;

        protected CompetitorWrapper(T competitor) {
            this._competitor = competitor;
        }

        @Override
        public CycleResult call() throws Exception {
            if (isCompleted())
                return CycleResult.TERMINATE;

            try {
                boolean lastIteration = isLastIteration();
                if (_competitor.execute(lastIteration)) {
                    onCompletion(_competitor, null);
                    return CycleResult.TERMINATE;
                }

                if (lastIteration) {
                    onCompletion(null, null);
                    return CycleResult.TERMINATE;
                }

                return CycleResult.IDLE_CONTINUE;
            } catch (Throwable e) {
                onCompletion(null, e);
                return CycleResult.TERMINATE;
            }
        }

        protected abstract boolean isLastIteration();

    }
}

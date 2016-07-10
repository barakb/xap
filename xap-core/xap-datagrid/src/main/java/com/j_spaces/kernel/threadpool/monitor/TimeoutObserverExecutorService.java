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

package com.j_spaces.kernel.threadpool.monitor;

import com.gigaspaces.internal.utils.concurrent.GSThread;
import com.gigaspaces.time.SystemTime;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * A timeout mechanism on top of an executor service. Can be used for tasks that may be interrupted
 * if an execution time has elapsed. Much like a {@link Future#get(long, TimeUnit)} but done by a
 * background thread.
 *
 * @author Moran Avigdor
 * @since 7.0.2
 */
@com.gigaspaces.api.InternalApi
public class TimeoutObserverExecutorService implements FutureExecutorService {

    private final ExecutorService executorService;
    private final long timeout;

    private final ObserverThread observerThread;
    private final LinkedBlockingQueue<Observable> queue = new LinkedBlockingQueue<Observable>();

    /**
     * Background thread harvesting obsolete futures
     */
    private final class ObserverThread extends GSThread {

        public ObserverThread() {
            super("TimeoutObserverExecutorService-ObserverThread");
        }

        @Override
        public void run() {
            try {
                while (!isInterrupted()) {
                    Thread.sleep(timeout);
                    harvestObsolete();
                }
            } catch (InterruptedException e) {
                //ignore and die
            }
        }

        private void harvestObsolete() {
            Iterator<Observable> iterator = queue.iterator();
            while (iterator.hasNext()) {
                Observable observable = iterator.next();
                if (observable.isObsolete()) {
                    iterator.remove();
                }
            }
        }

        public void close() {
            this.interrupt();
            harvestObsolete();
            queue.clear();
        }
    }

    /**
     * An observable future task, observed by an {@link ObserverThread} if obsolete
     */
    private static class Observable {
        private final Future<?> future;
        private final long expiration;

        public Observable(Future<?> future, long timeout) {
            this.future = future;
            this.expiration = SystemTime.timeMillis() + timeout;
        }

        /**
         * A future task is obsolete if it should not be monitored anymore, either because it has
         * has completed, canceled or timed-out.
         *
         * @return true if completed, canceled, or timed-out.
         */
        public boolean isObsolete() {
            if (future.isDone() || future.isCancelled())
                return true;
            if (expiration > SystemTime.timeMillis())
                return false;
            future.cancel(true);
            return true;
        }
    }

    /**
     * Constructs a timeout observer on top of an ExecutorService, exposing only executions of
     * {@link Future}s.
     *
     * @param executorService The underlying executor service
     * @param timeout         time in milliseconds to wait before interrupting a threads execution
     */
    public TimeoutObserverExecutorService(ExecutorService executorService, long timeout) {
        this.executorService = executorService;
        this.timeout = timeout;
        this.observerThread = new ObserverThread();
        this.observerThread.setDaemon(true);
        this.observerThread.start();
    }

    /**
     * add this observable future into the queue
     */
    private void observe(Future<?> future) {
        Observable observable = new Observable(future, timeout);
        queue.add(observable);
    }

    /*
     * @see com.j_spaces.kernel.threadpool.monitor.FutureExecutorService#invokeAll(java.util.Collection)
     */
    public <T> List<Future<T>> invokeAll(Collection<Callable<T>> tasks) throws InterruptedException {
        List<Future<T>> list = executorService.invokeAll(tasks);
        for (Future<T> future : list) {
            observe(future);
        }
        return list;
    }

    /*
     * @see com.j_spaces.kernel.threadpool.monitor.FutureExecutorService#invokeAll(java.util.Collection, long, java.util.concurrent.TimeUnit)
     */
    public <T> List<Future<T>> invokeAll(Collection<Callable<T>> tasks, long timeout, TimeUnit unit)
            throws InterruptedException {
        List<Future<T>> list = executorService.invokeAll(tasks, timeout, unit);
        for (Future<T> future : list) {
            observe(future);
        }
        return list;
    }

    /*
     * @see com.j_spaces.kernel.threadpool.monitor.FutureExecutorService#shutdown()
     */
    public void shutdown() {
        //first shutdown gracefully, meanwhile allowing the observable thread to run.
        executorService.shutdown();
        observerThread.close();
    }

    /*
     * @see com.j_spaces.kernel.threadpool.monitor.FutureExecutorService#shutdownNow()
     */
    public List<Runnable> shutdownNow() {
        //Call one last time the observable thread before forcing shutdown
        observerThread.close();
        return executorService.shutdownNow();
    }

    /*
     * @see com.j_spaces.kernel.threadpool.monitor.FutureExecutorService#submit(java.util.concurrent.Callable)
     */
    public <T> Future<T> submit(Callable<T> task) {
        Future<T> future = executorService.submit(task);
        observe(future);
        return future;
    }

    /*
     * @see com.j_spaces.kernel.threadpool.monitor.FutureExecutorService#submit(java.lang.Runnable, java.lang.Object)
     */
    public <T> Future<T> submit(Runnable task, T result) {
        Future<T> future = executorService.submit(task, result);
        observe(future);
        return future;
    }

    /*
     * @see com.j_spaces.kernel.threadpool.monitor.FutureExecutorService#submit(java.lang.Runnable)
     */
    public Future<?> submit(Runnable task) {
        Future<?> future = executorService.submit(task);
        observe(future);
        return future;
    }
}

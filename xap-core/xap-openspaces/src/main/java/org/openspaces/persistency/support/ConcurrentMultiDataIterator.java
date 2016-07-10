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


package org.openspaces.persistency.support;

import com.gigaspaces.datasource.DataIterator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A simple implementation wrapping several iterators and exposing them as concurrent iterator using
 * a fixed size thread pool. If a given iterator is a {@link org.openspaces.persistency.support.MultiDataIterator}
 * it will flatten it by getting its respective list of iterators.
 *
 * <p>Note, this implementation assumes that {@link #hasNext()} is called before {@link #next()} is
 * called. And that {@link #next()} is called only when {@link #hasNext()} returns
 * <code>true</code>.
 *
 * @author kimchy
 */
public class ConcurrentMultiDataIterator implements MultiDataIterator {

    private volatile boolean closed = false;

    final private int threadPoolSize;

    final private DataIterator[] iterators;

    final private BlockingQueue queue;

    final private DataIteratorRunnable[] runnables;

    final private AtomicInteger finishedRunnables = new AtomicInteger();

    private Object current;

    private int checkForExceptionsCounter = 0;

    private ExecutorService executor;

    private volatile RuntimeException exception;

    public ConcurrentMultiDataIterator(DataIterator[] iterators, int threadPoolSize) {
        ArrayList<DataIterator> itList = new ArrayList<DataIterator>();
        for (DataIterator iterator : iterators) {
            if (iterator instanceof MultiDataIterator) {
                itList.addAll(Arrays.asList(((MultiDataIterator) iterator).iterators()));
            } else {
                itList.add(iterator);
            }
        }
        this.iterators = itList.toArray(new DataIterator[itList.size()]);
        this.threadPoolSize = threadPoolSize;
        this.queue = new LinkedBlockingQueue();
        this.runnables = new DataIteratorRunnable[this.iterators.length];
    }

    public DataIterator[] iterators() {
        return this.iterators;
    }

    public boolean hasNext() {
        if (executor == null) {
            executor = Executors.newFixedThreadPool(threadPoolSize);
            for (int i = 0; i < iterators.length; i++) {
                runnables[i] = new DataIteratorRunnable(iterators[i]);
                executor.execute(runnables[i]);
            }
        }
        if (current != null) {
            return true;
        }
        while (true) {
            // every 100, check for exceptions
            if (checkForExceptionsCounter++ > 100) {
                checkForExceptionsCounter = 0;
                checkForExceptions();
            }
            try {
                current = queue.poll(100, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                // do nothing
            }
            if (current != null) {
                return true;
            }
            if (finishedRunnables.get() == iterators.length) {
                checkForExceptions();
                break;
            }
        }
        return false;
    }

    public Object next() {
        Object next = current;
        current = null;
        return next;
    }

    public void remove() {
        throw new UnsupportedOperationException("remove not supported");
    }

    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        for (DataIteratorRunnable runnable : runnables) {
            runnable.stop();
        }
        executor.shutdownNow();
    }

    private void checkForExceptions() {
        if (exception != null) {
            close();
            throw exception;
        }
    }

    private class DataIteratorRunnable implements Runnable {

        private volatile boolean running = true;

        final private DataIterator iterator;

        private DataIteratorRunnable(DataIterator iterator) {
            this.iterator = iterator;
        }

        public void run() {
            try {
                while (running && iterator.hasNext()) {
                    queue.put(iterator.next());
                }
            } catch (InterruptedException e) {
                exception = new RuntimeException(e);
            } catch (RuntimeException e) {
                exception = e;
            } finally {
                finishedRunnables.incrementAndGet();
                iterator.close();
            }
        }

        public void stop() {
            this.running = false;
        }
    }
}

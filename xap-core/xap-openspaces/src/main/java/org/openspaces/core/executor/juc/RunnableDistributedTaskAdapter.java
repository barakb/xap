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


package org.openspaces.core.executor.juc;

import com.gigaspaces.async.AsyncResult;
import com.gigaspaces.async.AsyncResultFilter;
import com.gigaspaces.async.AsyncResultFilterEvent;
import com.gigaspaces.async.AsyncResultsReducer;

import org.openspaces.core.executor.DistributedTask;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.List;

/**
 * An adapter allowing to execute a {@link Runnable} in a distributed fashion. In such a case, a
 * {@link com.gigaspaces.async.AsyncResultsReducer} must be implemented or provided (see the
 * different constructors). An optional {@link com.gigaspaces.async.AsyncResultFilter} can be also
 * implemented or provided.
 *
 * @author kimchy
 */
public class RunnableDistributedTaskAdapter<T extends Serializable, R> extends RunnableTaskAdapter<T>
        implements DistributedTask<T, R>, AsyncResultFilter<T> {

    private static final long serialVersionUID = 8361827094517059908L;

    private transient AsyncResultsReducer<T, R> reducer;

    private transient AsyncResultFilter<T> filter;

    /**
     * Here just for externalizable.
     */
    public RunnableDistributedTaskAdapter() {
    }

    /**
     * Constructs a new runnable distributed task adapter. The runnable in this case must implements
     * {link AsyncResultReducer} and optionally can implement {@link com.gigaspaces.async.AsyncResultFilter}.
     */
    public RunnableDistributedTaskAdapter(Runnable runnable) throws IllegalArgumentException {
        super(runnable);
        if (!(runnable instanceof AsyncResultsReducer)) {
            throw new IllegalArgumentException("Runnable must either implement the AsyncResultReducer interface, or provide it");
        }
        this.reducer = (AsyncResultsReducer<T, R>) runnable;
        if (runnable instanceof AsyncResultFilter) {
            this.filter = (AsyncResultFilter<T>) runnable;
        }
    }

    /**
     * Constructs a new runnable distributed task adapter. The runnable in this case must implements
     * {link AsyncResultReducer} and optionally can implement {@link com.gigaspaces.async.AsyncResultFilter}.
     */
    public RunnableDistributedTaskAdapter(Runnable runnable, T result) {
        super(runnable, result);
        if (!(runnable instanceof AsyncResultsReducer)) {
            throw new IllegalArgumentException("Runnable must either implement the AsyncResultReducer interface, or provide it");
        }
        this.reducer = (AsyncResultsReducer<T, R>) runnable;
        if (runnable instanceof AsyncResultFilter) {
            this.filter = (AsyncResultFilter<T>) runnable;
        }
    }

    /**
     * Constructs a new runnable distributed task adapter with a separate reducer. The reducer or
     * the runnable can optionally implement {@link com.gigaspaces.async.AsyncResultFilter}.
     */
    public RunnableDistributedTaskAdapter(Runnable runnable, AsyncResultsReducer<T, R> reducer) throws IllegalArgumentException {
        super(runnable);
        this.reducer = reducer;
        if (runnable instanceof AsyncResultFilter) {
            this.filter = (AsyncResultFilter<T>) runnable;
        }
        if (reducer instanceof AsyncResultFilter) {
            this.filter = (AsyncResultFilter<T>) reducer;
        }
    }

    /**
     * Constructs a new runnable distributed task adapter with a separate reducer. The reducer or
     * the runnable can optionally implement {@link com.gigaspaces.async.AsyncResultFilter}.
     */
    public RunnableDistributedTaskAdapter(Runnable runnable, T result, AsyncResultsReducer<T, R> reducer) throws IllegalArgumentException {
        super(runnable, result);
        this.reducer = reducer;
        if (runnable instanceof AsyncResultFilter) {
            this.filter = (AsyncResultFilter<T>) runnable;
        }
        if (reducer instanceof AsyncResultFilter) {
            this.filter = (AsyncResultFilter<T>) reducer;
        }
    }

    /**
     * Constructs a new runnable distributed task adapter with a separate reducer and filter.
     */
    public RunnableDistributedTaskAdapter(Runnable runnable, AsyncResultsReducer<T, R> reducer, AsyncResultFilter<T> filter) throws IllegalArgumentException {
        super(runnable);
        this.reducer = reducer;
        this.filter = filter;
    }

    /**
     * Constructs a new runnable distributed task adapter with a separate reducer and filter.
     */
    public RunnableDistributedTaskAdapter(Runnable runnable, T result, AsyncResultsReducer<T, R> reducer, AsyncResultFilter<T> filter) throws IllegalArgumentException {
        super(runnable, result);
        this.reducer = reducer;
        this.filter = filter;
    }

    /**
     * Delegates the call to the provided reducer.
     */
    public R reduce(List<AsyncResult<T>> results) throws Exception {
        return reducer.reduce(results);
    }

    /**
     * If a filter is provided in one of the constructor methods, will delegate the call to it.
     * Otherwise it will return the default {@link com.gigaspaces.async.AsyncResultFilter.Decision#CONTINUE}.
     */
    public Decision onResult(AsyncResultFilterEvent<T> event) {
        if (filter != null) {
            return filter.onResult(event);
        }
        return Decision.CONTINUE;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
    }
}
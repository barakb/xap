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
import java.util.concurrent.Callable;

/**
 * An adapter allowing to execute a {@link java.util.concurrent.Callable} in a distributed fashion.
 * In such a case, a {@link com.gigaspaces.async.AsyncResultsReducer} must be implemented or
 * provided (see the different constructors). An optional {@link com.gigaspaces.async.AsyncResultFilter}
 * can be also implemented or provided.
 *
 * @author kimchy
 */
public class CallableDistributedTaskAdapter<T extends Serializable, R> extends CallableTaskAdapter<T>
        implements DistributedTask<T, R>, AsyncResultFilter<T> {

    private static final long serialVersionUID = -5146382658283768618L;

    private transient AsyncResultsReducer<T, R> reducer;

    private transient AsyncResultFilter<T> filter;

    /**
     * Here just for externalizable.
     */
    public CallableDistributedTaskAdapter() {
    }

    /**
     * Constructs a new callable distributed task adapter. The callable in this case must implements
     * {link AsyncResultReducer} and optionally can implement {@link com.gigaspaces.async.AsyncResultFilter}.
     */
    public CallableDistributedTaskAdapter(Callable<T> callable) throws IllegalArgumentException {
        super(callable);
        if (!(callable instanceof AsyncResultsReducer)) {
            throw new IllegalArgumentException("Callable must either implement the AsyncResultReducer interface, or provide it");
        }
        this.reducer = (AsyncResultsReducer<T, R>) callable;
        if (callable instanceof AsyncResultFilter) {
            this.filter = (AsyncResultFilter<T>) callable;
        }
    }

    /**
     * Constructs a new callable distributed task adapter with a separate reducer. The reducer or
     * the callable can optionally implement {@link com.gigaspaces.async.AsyncResultFilter}.
     */
    public CallableDistributedTaskAdapter(Callable<T> callable, AsyncResultsReducer<T, R> reducer) throws IllegalArgumentException {
        super(callable);
        this.reducer = reducer;
        if (callable instanceof AsyncResultFilter) {
            this.filter = (AsyncResultFilter<T>) callable;
        }
        if (reducer instanceof AsyncResultFilter) {
            this.filter = (AsyncResultFilter<T>) reducer;
        }
    }

    /**
     * Constructs a new callable distributed task adapter with a separate reducer and filter.
     */
    public CallableDistributedTaskAdapter(Callable<T> callable, AsyncResultsReducer<T, R> reducer, AsyncResultFilter<T> filter) throws IllegalArgumentException {
        super(callable);
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

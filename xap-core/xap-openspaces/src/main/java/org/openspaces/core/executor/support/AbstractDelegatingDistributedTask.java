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


package org.openspaces.core.executor.support;

import com.gigaspaces.async.AsyncResultFilter;
import com.gigaspaces.async.AsyncResultFilterEvent;

import org.openspaces.core.executor.DistributedTask;
import org.openspaces.core.executor.Task;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;

/**
 * A base class for delegating tasks that are also distributed tasks
 *
 * @author kimchy
 */
public abstract class AbstractDelegatingDistributedTask<T extends Serializable, R> extends SimpleDelegatingTask<T>
        implements DistributedTask<T, R>, AsyncResultFilter<T> {

    private static final long serialVersionUID = 8936434181366749219L;

    private transient AsyncResultFilter<T> filter;

    protected AbstractDelegatingDistributedTask() {
        super();
    }

    public AbstractDelegatingDistributedTask(Task<T> task) {
        super(task);
        if (task instanceof AsyncResultFilter) {
            this.filter = (AsyncResultFilter<T>) task;
        }
    }

    public AbstractDelegatingDistributedTask(Task<T> task, AsyncResultFilter<T> filter) {
        super(task);
        this.filter = filter;
    }

    public Decision onResult(AsyncResultFilterEvent<T> event) {
        if (filter != null) {
            return filter.onResult(event);
        }
        return Decision.CONTINUE;
    }

    protected AsyncResultFilter<T> getFilter() {
        return filter;
    }

    @Override
    protected void _writeExternal(ObjectOutput output) throws IOException {
        super._writeExternal(output);
    }

    @Override
    protected void _readExternal(ObjectInput input) throws IOException, ClassNotFoundException {
        super._readExternal(input);
    }
}

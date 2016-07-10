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

import com.gigaspaces.async.AsyncResult;
import com.gigaspaces.async.AsyncResultFilter;

import org.openspaces.core.executor.DistributedTask;

import java.io.Serializable;
import java.util.List;

/**
 * A simple implementation of delegating distributed task that accepts the task to delegate to.
 *
 * @author kimchy
 */
public class SimpleDelegatingDistributedTask<T extends Serializable, R> extends AbstractDelegatingDistributedTask<T, R> {

    private static final long serialVersionUID = -48040005368590730L;

    protected SimpleDelegatingDistributedTask() {
        super();
    }

    public SimpleDelegatingDistributedTask(DistributedTask<T, R> task) {
        super(task);
    }

    public SimpleDelegatingDistributedTask(DistributedTask<T, R> task, AsyncResultFilter<T> filter) {
        super(task, filter);
    }

    public R reduce(List<AsyncResult<T>> asyncResults) throws Exception {
        return ((DistributedTask<T, R>) getDelegatedTask()).reduce(asyncResults);
    }
}
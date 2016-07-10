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


package org.openspaces.core.executor.internal;

import com.gigaspaces.async.AsyncResult;
import com.gigaspaces.async.AsyncResultFilter;
import com.gigaspaces.async.AsyncResultFilterEvent;
import com.gigaspaces.executor.DistributedSpaceTask;

import org.openspaces.core.executor.DistributedTask;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.List;

/**
 * An internal implementation of {@link com.gigaspaces.executor.DistributedSpaceTask} that wraps the
 * actual {@link org.openspaces.core.executor.Task} to be executed.
 *
 * @author kimchy
 */
public class InternalDistributedSpaceTaskWrapper<T extends Serializable, R> extends InternalSpaceTaskWrapper<T>
        implements DistributedSpaceTask<T, R>, AsyncResultFilter<T> {

    private static final long serialVersionUID = -8997841035295544425L;

    public InternalDistributedSpaceTaskWrapper() {
    }

    public InternalDistributedSpaceTaskWrapper(DistributedTask<T, R> task) {
        super(task, null);
    }

    @SuppressWarnings("unchecked")
    public R reduce(List<AsyncResult<T>> asyncResults) throws Exception {
        return (R) ((DistributedTask) getTask()).reduce(asyncResults);
    }

    public Decision onResult(AsyncResultFilterEvent<T> event) {
        if (getTask() instanceof AsyncResultFilter) {
            return ((AsyncResultFilter) getTask()).onResult(event);
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
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


package org.openspaces.events.support;

import com.gigaspaces.async.AsyncResult;

import org.openspaces.core.executor.DistributedTask;

import java.io.Serializable;
import java.util.List;

/**
 * A task that registers a dynamically added event container in a distributed manner. (using {@link
 * org.openspaces.events.polling.Polling} or {@link org.openspaces.events.notify.Notify}.
 *
 * @author kimchy
 */
public class RegisterDistributedEventContainerTask extends RegisterEventContainerTask implements DistributedTask<Serializable, Object> {

    private static final long serialVersionUID = 463040102712604939L;

    protected RegisterDistributedEventContainerTask() {
        super();
    }

    public RegisterDistributedEventContainerTask(String containerName) {
        super(containerName);
    }

    public Object reduce(List<AsyncResult<Serializable>> asyncResults) throws Exception {
        return null;
    }
}
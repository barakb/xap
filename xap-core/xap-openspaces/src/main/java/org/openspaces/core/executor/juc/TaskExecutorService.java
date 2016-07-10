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

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * An extended interface to {@link ExecutorService}.
 *
 * @author kimchy
 */
public interface TaskExecutorService extends ExecutorService {

    /**
     * Submits a callabale to to be executed on the Space using the provided routing.
     *
     * @see org.openspaces.core.GigaSpace#execute(org.openspaces.core.executor.Task, Object)
     */
    <T> Future<T> submit(Callable<T> task, Object routing);
}

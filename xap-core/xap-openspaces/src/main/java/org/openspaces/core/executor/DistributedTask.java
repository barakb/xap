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


package org.openspaces.core.executor;

import com.gigaspaces.async.AsyncResultsReducer;

import java.io.Serializable;

/**
 * A distributed task is a {@link org.openspaces.core.executor.Task} that is executed on several
 * space nodes, requiring to {@link #reduce(java.util.List)} the list of {@link
 * com.gigaspaces.async.AsyncResult}s.
 *
 * @author kimchy
 */
public interface DistributedTask<T extends Serializable, R> extends Task<T>, AsyncResultsReducer<T, R> {
}

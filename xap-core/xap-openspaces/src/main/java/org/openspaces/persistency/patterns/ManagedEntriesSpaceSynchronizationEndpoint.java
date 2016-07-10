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

package org.openspaces.persistency.patterns;

import com.gigaspaces.sync.SpaceSynchronizationEndpoint;

/**
 * An implementation of {@link com.gigaspaces.datasource.SpaceSynchronizationEndpoint} that can
 * provide a list of all the given entries this synchronization endpoint interceptor handles.
 *
 * @author eitany
 * @since 9.5
 */
public abstract class ManagedEntriesSpaceSynchronizationEndpoint extends SpaceSynchronizationEndpoint {

    public abstract Iterable<String> getManagedEntries();

}

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

package com.gigaspaces.internal.client.cache;

import com.gigaspaces.internal.client.spaceproxy.IDirectSpaceProxy;
import com.j_spaces.core.IJSpace;

/**
 * A space cache implementation provides local cache support such as local view and local cache.
 *
 * @author kimchy
 * @since 6.0
 */
public interface ISpaceCache {
    /**
     * Closes space cache (the local cache). Note, any operations performed on the space cache after
     * it has been closed will result in an error. For example, and event session built on top of
     * the space cache should be closed before the space cache is closed.
     */
    void close();

    /**
     * Returns a direct proxy to the remote space.
     */
    IJSpace getRemoteSpace();

    /**
     * Returns a direct proxy to the local space.
     */
    IDirectSpaceProxy getLocalSpace();
}

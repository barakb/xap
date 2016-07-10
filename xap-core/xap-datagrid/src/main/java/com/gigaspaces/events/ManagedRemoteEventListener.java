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

package com.gigaspaces.events;

import net.jini.core.event.RemoteEventListener;

/**
 * this interface allows the listener to receive an initialization event.
 *
 * @author asy ronen
 * @version 1.0
 * @since 6.0
 */
public interface ManagedRemoteEventListener extends RemoteEventListener {
    /**
     * Recursive initialization of managed listener.
     */
    void init(GSEventRegistration registration);

    /**
     * lifecycle recursive callback for the managed listener.
     */
    void shutdown(GSEventRegistration registration);
}

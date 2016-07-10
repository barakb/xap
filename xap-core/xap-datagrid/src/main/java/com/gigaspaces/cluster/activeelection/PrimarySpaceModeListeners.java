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

package com.gigaspaces.cluster.activeelection;

import com.gigaspaces.internal.utils.collections.CopyOnUpdateSet;

import java.util.Iterator;

/**
 * Listeners for space stand by events. This is copy on update in order to avoid self concurrent
 * modification exception when there's a space mode event registration in a listener
 * implementation.
 */
@com.gigaspaces.api.InternalApi
public class PrimarySpaceModeListeners {

    private final CopyOnUpdateSet<ISpaceModeListener> _listeners = new CopyOnUpdateSet<ISpaceModeListener>();

    /**
     * Add listener for space availability
     */
    public void addListener(ISpaceModeListener listener) {
        _listeners.add(listener);
    }

    /**
     * Removes a listener for space availability
     */
    public void removeListener(ISpaceModeListener listener) {
        _listeners.remove(listener);
    }

    /**
     * @return a safe from concurrent updates iterator
     */
    public Iterator<ISpaceModeListener> iterator() {
        return _listeners.iterator();
    }

    /**
     * Remove all listeners
     */
    public void clear() {
        _listeners.clear();
    }
}

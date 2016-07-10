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

package com.j_spaces.kernel.weaklistener;

import net.jini.core.event.RemoteEvent;
import net.jini.core.event.RemoteEventListener;
import net.jini.core.event.UnknownEventException;

import java.lang.ref.WeakReference;
import java.rmi.RemoteException;

/**
 * Used in order to weakly hold listeners by the Notify delegator.
 *
 * @author Guy Korland
 * @version 1.0
 * @since 5.01
 */
@com.gigaspaces.api.InternalApi
public class WeakRemoteEventListener extends WeakReference<RemoteEventListener>
        implements RemoteEventListener {
    public WeakRemoteEventListener(RemoteEventListener listener) {
        super(listener);
    }

    /**
     * Delegate the notification to the listener.
     */
    public void notify(RemoteEvent event) throws UnknownEventException, RemoteException {
        RemoteEventListener l = get();
        if (l != null) {
            l.notify(event);
        }
    }
}

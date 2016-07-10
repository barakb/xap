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

import net.jini.discovery.DiscoveryEvent;
import net.jini.discovery.DiscoveryListener;

import java.lang.ref.WeakReference;

/**
 * Used in order to weakly hold listeners by the {@link LookupDiscoveryManager}.
 *
 * @author moran
 * @version 1.0
 * @since 6.0
 */
@com.gigaspaces.api.InternalApi
public class WeakDiscoveryListener extends WeakReference<DiscoveryListener>
        implements DiscoveryListener {
    /**
     * Weak DiscoveryListener constructor.
     *
     * @param listener the DiscoveryListener reference.
     */
    public WeakDiscoveryListener(DiscoveryListener listener) {
        super(listener);
    }

    /**
     * Delegate the notification to the listener.
     *
     * @see net.jini.discovery.DiscoveryListener#discarded(net.jini.discovery.DiscoveryEvent)
     */
    public void discarded(DiscoveryEvent event) {
        DiscoveryListener l = get();
        if (l != null) {
            l.discarded(event);
        }

    }

    /**
     * Delegate the notification to the listener.
     *
     * @see net.jini.discovery.DiscoveryListener#discovered(net.jini.discovery.DiscoveryEvent)
     */
    public void discovered(DiscoveryEvent event) {
        DiscoveryListener l = get();
        if (l != null) {
            l.discovered(event);
        }
    }
}

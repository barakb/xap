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


package org.openspaces.events.notify;

/**
 * Custom Communication type is deprecated since 9.7 - the default is multiplex and there are no
 * benefits for using unicast.
 *
 * @deprecated Since 9.7
 */
@Deprecated
public enum NotifyComType {

    /**
     * Controls how notification are propagated from the space to the listener. Unicast propagation
     * uses TCP unicast communication which is usually best for small amount of registered clients.
     * This is the default communication type.
     */
    UNICAST(SimpleNotifyEventListenerContainer.COM_TYPE_UNICAST),

    /**
     * Controls how notification are propagated from the space to the listener. Same as unicast
     * ({@link #UNICAST}) in terms of communication protocol but uses a single client side
     * multiplexer which handles all the dispatching to the different notification listeners.
     */
    MULTIPLEX(SimpleNotifyEventListenerContainer.COM_TYPE_MULTIPLEX),

    /**
     * Multicast notifications are no longer supported. This enum value will be removed in future
     * versions.
     *
     * @deprecated Since 9.0.0
     */
    @Deprecated
    MULTICAST(SimpleNotifyEventListenerContainer.COM_TYPE_MULTICAST);


    private final int value;


    NotifyComType(int value) {
        this.value = value;
    }

    public int value() {
        return value;
    }
}

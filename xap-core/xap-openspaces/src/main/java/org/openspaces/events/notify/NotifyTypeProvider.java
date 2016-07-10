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

import org.openspaces.events.SpaceDataEventListener;

/**
 * An extension of space event listener allowing the listener to control programmatically (without
 * the user having to configure it within the notify container) which notifications this listener
 * will be invoked on.
 *
 * <p>All markers return <code>Boolean</code> value. <code>null</code> means that it will have no
 * affect on the flag appropriate flag.
 *
 * <p>Note, all flags will only take place if it is not overridden by the user when configuring the
 * notify container. If the user has set, for example, the write notify flag, then this provider
 * {@link #isWrite()} will not be taken into account. This allows for advance users to further
 * configure a "recommended" notifications types for a specific listener that implements this
 * interface.
 *
 * @author kimchy
 */
public interface NotifyTypeProvider extends SpaceDataEventListener {

    /**
     * Should this listener be notified on write operations. <code>null</code> will leave the flag
     * un changed.
     */
    Boolean isWrite();

    /**
     * Should this listener be notified on update operations. <code>null</code> will leave the flag
     * un changed.
     */
    Boolean isUpdate();

    /**
     * Should this listener be notified on lease expiration operations. <code>null</code> will leave
     * the flag un changed.
     */
    Boolean isLeaseExpire();

    /**
     * Should this listener be notified on take operations. <code>null</code> will leave the flag un
     * changed.
     */
    Boolean isTake();

    /**
     * Should this listener be notified when unmatched templates events occur. <code>null</code>
     * will leave the flag un changed.
     */
    Boolean isUnmatched();

    /**
     * Should this listener be notified when matched templates events occur. <code>null</code> will
     * leave the flag unchanged.
     *
     * @since 9.1
     */
    Boolean isMatchedUpdate();

    /**
     * Should this listener be notified when re-matched templates events occur. <code>null</code>
     * will leave the flag unchanged.
     *
     * @since 9.1
     */
    Boolean isRematchedUpdate();
}

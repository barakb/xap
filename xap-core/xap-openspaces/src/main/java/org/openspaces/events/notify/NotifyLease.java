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

import com.gigaspaces.events.EventSessionConfig;

import net.jini.core.lease.Lease;
import net.jini.lease.LeaseListener;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marking with this annotation will cause the notify container to have a lease (automatically sets
 * <code>true</code> to {@link org.openspaces.events.notify.SimpleNotifyEventListenerContainer#setAutoRenew(boolean)}.
 *
 * @author kimchy
 * @deprecated Since 9.7 - event listener with custom lease or custom auto-renew is deprecated.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Deprecated
public @interface NotifyLease {

    /**
     * Controls the lease associated with the registered listener. Defaults to {@link
     * net.jini.core.lease.Lease#FOREVER}.
     *
     * @see org.openspaces.events.notify.SimpleNotifyEventListenerContainer#setListenerLease(long)
     */
    long lease() default Lease.FOREVER;

    /**
     * The period of time your notifications stop being renewed.
     */
    long renewExpiration() default EventSessionConfig.DEFAULT_RENEW_EXPIRATION;

    /**
     * The period of time that passes between client failure, and the time your notifications stop
     * being sent. use more than renewRTT.
     */
    long renewDuration() default EventSessionConfig.DEFAULT_RENEW_DURATION;

    /**
     * RoundTripTime - the time that takes to reach the server and return.
     */
    long renewRTT() default EventSessionConfig.DEFAULT_RENEW_RTT;

    /**
     * Sets the lease listener for the lease. Default to no listener.
     *
     * @see SimpleNotifyEventListenerContainer#setLeaseListener(net.jini.lease.LeaseListener)
     */
    Class<? extends LeaseListener> leaseListener() default net.jini.lease.LeaseListener.class;
}

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

package com.gigaspaces.events.lease;

import com.gigaspaces.events.EventSessionConfig;
import com.gigaspaces.time.SystemTime;

import net.jini.core.event.EventRegistration;
import net.jini.core.lease.UnknownLeaseException;
import net.jini.lease.LeaseListener;
import net.jini.lease.LeaseRenewalManager;

/**
 * Jini-based implementation of managing renewal of event registration leases.
 *
 * @author Niv Ingberg
 * @since 9.7.0
 */
@com.gigaspaces.api.InternalApi
public class JiniEventLeaseRenewalManager implements EventLeaseRenewalManager {

    private final EventSessionConfig config;
    private final LeaseRenewalManager leaseRenewalManager;

    public JiniEventLeaseRenewalManager(EventSessionConfig config) {
        super();
        this.config = config;
        this.leaseRenewalManager = new LeaseRenewalManager(config.getRenewRTT(), 2);
    }

    @Override
    public boolean supportsCustomLease() {
        return true;
    }

    @Override
    public void close() {
        leaseRenewalManager.terminate();
    }

    @Override
    public long getRenewalDuration() {
        return config.getRenewDuration();
    }

    @Override
    public void registerAutoRenew(EventRegistration eventRegistration, LeaseListener listener) {
        final long systemTime = SystemTime.timeMillis();
        // if systemTime + _config.getRenewExpiration() overflow Long.MAX_VALUE use Long.MAX_VALUE instead.
        final long desiredExpiration = systemTime < Long.MAX_VALUE - config.getRenewExpiration()
                ? systemTime + config.getRenewExpiration()
                : Long.MAX_VALUE;

        leaseRenewalManager.renewUntil(eventRegistration.getLease(), desiredExpiration, config.getRenewDuration(), listener);
    }

    @Override
    public void unregisterAutoRenew(EventRegistration eventRegistration) {
        try {
            leaseRenewalManager.remove(eventRegistration.getLease());
        } catch (UnknownLeaseException e) {
            // lease already expired - ignore exception.
        }
    }
}

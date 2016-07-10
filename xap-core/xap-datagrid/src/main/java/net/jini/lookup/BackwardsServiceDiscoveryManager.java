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
package net.jini.lookup;

import net.jini.config.Configuration;
import net.jini.config.ConfigurationException;
import net.jini.core.lease.Lease;
import net.jini.discovery.DiscoveryManagement;
import net.jini.lease.LeaseRenewalManager;

import java.io.IOException;

/**
 * This class is used as a backwards compatible implementation for ServiceDiscoveryManager before
 * the changes introduced by GS-9875
 *
 * @author Itai Frenkel
 */
@Deprecated
@com.gigaspaces.api.InternalApi
public class BackwardsServiceDiscoveryManager extends ServiceDiscoveryManager {

    public BackwardsServiceDiscoveryManager(DiscoveryManagement discoveryMgr,
                                            LeaseRenewalManager leaseMgr, Configuration config)
            throws IOException, ConfigurationException {
        super(discoveryMgr, leaseMgr, config);
    }

    private static final long BACKWARDS_COMPATIBLE_NOTIFICATIONS_LEASE_RENEW_RATE = Lease.FOREVER;
    private static final long BACKWARDS_COMPATIBLE_REMOVE_SERVICE_IF_ORPHAN_DELAY = 0;

    @Override
    protected long getDefaultNotificationsLeaseRenewalRate() {
        return BACKWARDS_COMPATIBLE_NOTIFICATIONS_LEASE_RENEW_RATE;
    }

    @Override
    protected long getDefaultRemoveServiceIfOrphanDelay() {
        return BACKWARDS_COMPATIBLE_REMOVE_SERVICE_IF_ORPHAN_DELAY;
    }

}
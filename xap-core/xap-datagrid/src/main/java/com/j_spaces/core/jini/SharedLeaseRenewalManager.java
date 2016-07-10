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

package com.j_spaces.core.jini;

import com.j_spaces.core.service.ServiceConfigLoader;

import net.jini.config.ConfigurationException;
import net.jini.lease.LeaseRenewalManager;

/**
 * A singleton instance of Jini {@link net.jini.lease.LeaseRenewalManager} configured using {@link
 * com.j_spaces.core.service.ServiceConfigLoader#getConfiguration()}.
 *
 * @author kimchy
 */
@com.gigaspaces.api.InternalApi
public class SharedLeaseRenewalManager {

    private static LeaseRenewalManager leaseRenewalManager;

    private static int instnaceCounter = 0;

    public static synchronized LeaseRenewalManager getLeaseRenewalManager() throws ConfigurationException {
        if (leaseRenewalManager == null) {
            leaseRenewalManager = new LeaseRenewalManager(ServiceConfigLoader.getConfiguration());
            instnaceCounter = 0;
        }
        instnaceCounter++;
        return leaseRenewalManager;
    }

    public static synchronized void releaseLeaseRenewalManaer() {
        if (--instnaceCounter <= 0 && leaseRenewalManager != null) {
            leaseRenewalManager.terminate();
            leaseRenewalManager = null;
        }
    }
}

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

package com.gigaspaces.admin.discovery;

import net.jini.core.discovery.LookupLocator;
import net.jini.core.lookup.ServiceID;
import net.jini.core.lookup.ServiceRegistrar;

import java.rmi.RemoteException;

/**
 * Class to hold basic information for a discovered/discarded reggie
 */
@com.gigaspaces.api.InternalApi
public class ReggieStat {
    long eventTime;
    long baseTime;
    String[] groups;
    String machine;
    int port;
    int type;
    ServiceID serviceID;
    public final static int DISCOVERED = 0;
    public final static int DISCARDED = 1;

    /**
     * Create a RegieStat object
     *
     * @param type   Either <code>ReggieStat.DISCOVERED</code> or <code>ReggieStat.DISCARDED</code>
     * @param t      The time the event occurred
     * @param reggie The ServiceRegistrar instance being recorded
     * @throws RemoteException If there are comunication exceptions obtaining information from the
     *                         ServiceRegistrar instance
     */
    ReggieStat(int type, long t, ServiceRegistrar reggie, String[] groups)
            throws RemoteException {
        if (type < DISCOVERED || type > DISCARDED)
            throw new IllegalArgumentException("bad type");
        if (reggie == null)
            throw new NullPointerException("reggie is null");
        this.type = type;
        eventTime = t;
        this.groups = groups;
        LookupLocator locator = reggie.getLocator();
        machine = locator.getHost();
        port = locator.getPort();
        serviceID = reggie.getServiceID();
    }

    /**
     * Convenience method to obtain the time that the ServiceRegistar was discarded, provided a new
     * "base time" to establish (re-)discovery time
     */
    public long getBaseTime() {
        return (baseTime);
    }

    /**
     * Convenience method to obtain the host the ServiceRegistrar is on
     */
    public String getMachine() {
        return (machine);
    }

    /**
     * Convenience method to obtain the port the ServiceRegistrar is listening on
     */
    public int getPort() {
        return (port);
    }

    /**
     * Convenience method to obtain pre-fetced group names
     */
    public String[] getGroups() {
        return (groups);
    }

    /**
     * Get the time the event occurred
     */
    public long getEventTime() {
        return (eventTime);
    }

    boolean groupsMatch(ReggieStat rStat) {
        if (rStat.groups == null && groups == null)
            return (true);
        if (rStat.groups != null && groups == null)
            return (false);
        if (rStat.groups == null && groups != null)
            return (false);
        if (rStat.groups.length != groups.length)
            return (false);
        for (int i = 0; i < rStat.groups.length; i++) {
            boolean found = false;
            for (int j = 0; j < groups.length; j++) {
                if (groups[j].equals(rStat.groups[i])) {
                    found = true;
                    break;
                }
            }
            if (!found)
                return (false);
        }
        return (true);
    }
}
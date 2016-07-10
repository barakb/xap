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

package com.gigaspaces.internal.lease;

import com.gigaspaces.internal.client.spaceproxy.IDirectSpaceProxy;
import com.sun.jini.lease.AbstractLeaseMap;

import net.jini.core.lease.LeaseMapException;

import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A LeaseMap implementation using a space proxy to batch update leases.
 *
 * @author Niv Ingberg
 * @since 9.7.0
 */
@com.gigaspaces.api.InternalApi
public class SpaceLeaseMap extends AbstractLeaseMap {

    private final IDirectSpaceProxy _spaceProxy;

    public SpaceLeaseMap(IDirectSpaceProxy spaceProxy, boolean concurrent) {
        super(concurrent ? new ConcurrentHashMap() : new HashMap());
        this._spaceProxy = spaceProxy;
    }

    @Override
    public boolean canContainKey(Object key) {
        if (key == null || !(key instanceof SpaceLease))
            return false;
        return ((SpaceLease) key).canBatch(_spaceProxy);
    }

    @Override
    public void renewAll() throws LeaseMapException {
        updateAll(true);
    }

    @Override
    public void cancelAll() throws LeaseMapException {
        updateAll(false);
    }

    private void updateAll(boolean isRenew) throws LeaseMapException {
        LeaseUpdateBatch batch = createBatch(isRenew);
        Map<SpaceLease, Throwable> errors = LeaseUtils.updateBatch(_spaceProxy, batch);
        if (errors != null) {
            for (SpaceLease lease : errors.keySet())
                if (remove(lease) == null)
                    throw new ConcurrentModificationException();

            throw new LeaseMapException("Failed to " + (isRenew ? "renew" : "cancel") + " one or more leases", errors);
        }
    }

    private LeaseUpdateBatch createBatch(boolean isRenew) {
        final int size = this.size();
        final SpaceLease[] leases = new SpaceLease[size];
        final LeaseUpdateDetails[] leasesUpdateDetails = new LeaseUpdateDetails[size];

        Iterator<Map.Entry<SpaceLease, Long>> iterator = entrySet().iterator();
        for (int i = 0; iterator.hasNext(); i++) {
            Map.Entry<SpaceLease, Long> entry = iterator.next();
            leases[i] = entry.getKey();
            leasesUpdateDetails[i] = new LeaseUpdateDetails(leases[i], isRenew ? entry.getValue() : LeaseUtils.DISCARD_LEASE);
        }

        return new LeaseUpdateBatch(leases, leasesUpdateDetails, isRenew);
    }
}

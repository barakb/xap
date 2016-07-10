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

/**
 * @author Niv Ingberg
 * @since 9.7.0
 */
@com.gigaspaces.api.InternalApi
public class LeaseUpdateBatch {
    private final SpaceLease[] leases;
    private final LeaseUpdateDetails[] leasesUpdateDetails;
    private final boolean isRenew;

    public LeaseUpdateBatch(SpaceLease[] leases, LeaseUpdateDetails[] leasesUpdateDetails, boolean isRenew) {
        this.leases = leases;
        this.leasesUpdateDetails = leasesUpdateDetails;
        this.isRenew = isRenew;
    }

    public int getSize() {
        return leases.length;
    }

    public SpaceLease[] getLeases() {
        return leases;
    }

    public LeaseUpdateDetails[] getLeasesUpdateDetails() {
        return leasesUpdateDetails;
    }

    public boolean isRenew() {
        return isRenew;
    }
}

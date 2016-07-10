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

import com.gigaspaces.internal.utils.Textualizable;
import com.gigaspaces.internal.utils.Textualizer;

/**
 * @author Niv Ingberg
 * @since 9.7.0
 */
@com.gigaspaces.api.InternalApi
public class LeaseUpdateDetails implements Textualizable {
    private String uid;
    private String typeName;
    private int leaseObjectType;
    private Object routingValue;
    private long renewDuration;

    public LeaseUpdateDetails() {
    }

    public LeaseUpdateDetails(SpaceLease lease, long renewDuration) {
        this.uid = lease.getUID();
        this.typeName = lease.getTypeName();
        this.leaseObjectType = lease.getLeaseObjectType();
        this.routingValue = lease._routingValue;
        this.renewDuration = renewDuration;
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public String getTypeName() {
        return typeName;
    }

    public void setTypeName(String typeName) {
        this.typeName = typeName;
    }

    public int getLeaseObjectType() {
        return leaseObjectType;
    }

    public void setLeaseObjectType(int leaseObjectType) {
        this.leaseObjectType = leaseObjectType;
    }

    public Object getRoutingValue() {
        return routingValue;
    }

    public long getRenewDuration() {
        return renewDuration;
    }

    public void setRenewDuration(long renewDuration) {
        this.renewDuration = renewDuration;
    }

    @Override
    public void toText(Textualizer textualizer) {
        textualizer.append("uid", uid);
        textualizer.append("typeName", typeName);
        textualizer.append("leaseObjectType", leaseObjectType);
        textualizer.append("routingValue", routingValue);
        textualizer.append("renewDuration", renewDuration);
    }
}

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


package org.openspaces.core.transaction.manager;

import net.jini.lease.LeaseListener;

/**
 * Allows to configure transactions to be renewed automatically when using {@link
 * AbstractJiniTransactionManager} and its different sub-classes.
 *
 * <p>Will cause a transaction to be renewed each {@link #setRenewDuration(long)} (which should be
 * lower than the transaction timeout). Defaults to 2 seconds.
 *
 * <p>Allows also to set the {@link #setRenewRTT(long)} which controls the expected round trip time
 * to the server. Should be lower than the then the renew duration. Defaults to 1 second.
 *
 * <p>The transaction renewal also allows to configure the pool size of renewal managers that will
 * be used to renew transactions. It defaults to 1 and under heavy load of transactions with
 * renewals should probably be higher.
 *
 * @author kimchy
 */
public class TransactionLeaseRenewalConfig {

    private int poolSize = 1;

    private long renewRTT = 1000;

    private long renewDuration = 2000;

    private LeaseListener leaseListener;

    /**
     * Controls how often a transaction will be renewed (which should be lower than the transaction
     * timeout). This value is set in milliseconds and defaults to 2 seconds.
     */
    public long getRenewDuration() {
        return renewDuration;
    }

    /**
     * Controls how often a transaction will be renewed (which should be lower than the transaction
     * timeout). This value is set in milliseconds and defaults to 2 seconds.
     */
    public void setRenewDuration(long renewDuration) {
        this.renewDuration = renewDuration;
    }

    /**
     * Controls the expected round trip time to the server. This value is set in milliseconds and
     * defaults to 1 second. The value should be lower than the renew duration.
     */
    public long getRenewRTT() {
        return renewRTT;
    }

    /**
     * Controls the expected round trip time to the server. This value is set in milliseconds and
     * defaults to 1 second. The value should be lower than the renew duration.
     */
    public void setRenewRTT(long renewRTT) {
        this.renewRTT = renewRTT;
    }

    /**
     * Sets an optional renew listener to be notified on renew events.
     */
    public LeaseListener getLeaseListener() {
        return leaseListener;
    }

    /**
     * Sets an optional renew listener to be notified on renew events.
     */
    public void setLeaseListener(LeaseListener leaseListener) {
        this.leaseListener = leaseListener;
    }

    /**
     * The pool size value of lease renewal managers (responsible for renewing transactions).
     * Defaults to 1 and under heavy load should be higher.
     */
    public int getPoolSize() {
        return poolSize;
    }

    /**
     * The pool size value of lease renewal managers (responsible for renewing transactions).
     * Defaults to 1 and under heavy load should be higher.
     */
    public void setPoolSize(int poolSize) {
        this.poolSize = poolSize;
    }
}

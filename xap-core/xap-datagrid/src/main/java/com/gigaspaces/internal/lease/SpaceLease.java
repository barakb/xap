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
import com.gigaspaces.internal.client.spaceproxy.operations.UpdateLeaseSpaceOperationRequest;
import com.gigaspaces.internal.utils.Textualizable;
import com.gigaspaces.internal.utils.Textualizer;
import com.j_spaces.core.exception.internal.InterruptedSpaceException;

import net.jini.core.lease.Lease;
import net.jini.core.lease.LeaseDeniedException;
import net.jini.core.lease.LeaseMap;
import net.jini.core.lease.UnknownLeaseException;

import java.rmi.RemoteException;

/**
 * @author Niv Ingberg
 * @since 9.0.0
 */
public abstract class SpaceLease implements Lease, Textualizable {
    protected final IDirectSpaceProxy _spaceProxy;
    protected final String _typeName;
    protected final String _uid;
    protected final Object _routingValue;
    protected long _expirationTime;
    protected int _serialFormat;

    public SpaceLease(IDirectSpaceProxy spaceProxy, String typeName, String uid, Object routingValue, long expiration) {
        this._spaceProxy = spaceProxy;
        this._typeName = typeName;
        this._uid = uid;
        this._routingValue = routingValue;
        this._expirationTime = expiration;
        this._serialFormat = Lease.DURATION;
    }


    @Override
    public String toString() {
        return Textualizer.toString(this);
    }

    @Override
    public void toText(Textualizer textualizer) {
        textualizer.append("uid", _uid);
        textualizer.append("typeName", _typeName);
        textualizer.append("routingValue", _routingValue);
        textualizer.append("expirationTime", _expirationTime);
    }

    @Override
    public long getExpiration() {
        return _expirationTime;
    }

    @Override
    public int getSerialFormat() {
        return _serialFormat;
    }

    @Override
    public void setSerialFormat(int serialFormat) {
        if (serialFormat != Lease.DURATION && serialFormat != Lease.ABSOLUTE)
            throw new IllegalArgumentException("Invalid serial format: " + serialFormat);
        _serialFormat = serialFormat;
    }

    @Override
    public void cancel() throws UnknownLeaseException, RemoteException {
        updateLease(LeaseUtils.DISCARD_LEASE);
    }

    @Override
    public void renew(long duration) throws LeaseDeniedException,
            UnknownLeaseException, RemoteException {
        updateLease(duration);
        _expirationTime = LeaseUtils.toExpiration(duration);
    }

    private void updateLease(long duration) throws RemoteException, UnknownLeaseException {
        UpdateLeaseSpaceOperationRequest request = new UpdateLeaseSpaceOperationRequest(_uid, _typeName, getLeaseObjectType(), duration, _routingValue);
        try {
            _spaceProxy.getProxyRouter().execute(request);
        } catch (InterruptedException e) {
            throw new InterruptedSpaceException(e);
        }

        request.getFinalResult().processExecutionException();
    }

    @Override
    public LeaseMap createLeaseMap(long duration) {
        LeaseMap leaseMap = new SpaceLeaseMap(_spaceProxy, false);
        leaseMap.put(this, duration);
        return leaseMap;
    }

    @Override
    public boolean canBatch(Lease lease) {
        if (!(lease instanceof SpaceLease))
            return false;
        SpaceLease other = (SpaceLease) lease;
        return canBatch(other._spaceProxy);
    }

    public boolean canBatch(IDirectSpaceProxy spaceProxy) {
        // TODO: Reconsider using equals instead of same.
        return this._spaceProxy == spaceProxy;
    }

    public String getTypeName() {
        return _typeName;
    }

    public String getUID() {
        return _uid;
    }

    protected abstract int getLeaseObjectType();
}

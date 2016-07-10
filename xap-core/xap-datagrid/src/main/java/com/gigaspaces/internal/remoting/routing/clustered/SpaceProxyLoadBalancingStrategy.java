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

package com.gigaspaces.internal.remoting.routing.clustered;

import com.gigaspaces.internal.remoting.RemoteOperationRequest;
import com.gigaspaces.internal.remoting.routing.RemoteOperationRouterException;

import java.rmi.RemoteException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author Niv Ingberg
 * @since 9.1.2
 */
public abstract class SpaceProxyLoadBalancingStrategy {
    protected final RemoteOperationsExecutorsCluster _cluster;
    protected final Logger _logger;
    protected final Object _lock;

    public SpaceProxyLoadBalancingStrategy(RemoteOperationsExecutorsCluster cluster) {
        this._cluster = cluster;
        this._logger = _cluster.getLogger();
        this._lock = new Object();
    }

    public abstract RemoteOperationsExecutorProxy getCandidate(RemoteOperationRequest<?> request);

    public RemoteOperationsExecutorProxy findActiveMember(RemoteOperationRequest<?> request, long initialFailureTime, RemoteOperationsExecutorProxy oldCandidate)
            throws InterruptedException {
        synchronized (_lock) {
            // Double check candidate:
            RemoteOperationsExecutorProxy newCandidate = getCandidate(request);
            if (newCandidate != oldCandidate && newCandidate != null) {
                if (_logger.isLoggable(Level.FINE))
                    _logger.log(Level.FINE, "Active server" + _cluster.getPartitionDesc() + " was updated from " + (oldCandidate == null ? "null" : oldCandidate.getName()) + " to " + newCandidate.getName());
                _cluster.disconnect(oldCandidate);
                return newCandidate;
            }

            // Recalculate remaining time - might have changed while waiting for sync block:
            long remainingTime = _cluster.getRemainingTime(request, initialFailureTime);
            if (remainingTime <= 0) {
                if (_logger.isLoggable(Level.FINE))
                    _logger.log(Level.FINE, "Timeout expired while searching for active server" + _cluster.getPartitionDesc() + " " + _cluster.getElapsedTime(initialFailureTime));
                return null;
            }

            try {
                RemoteOperationsExecutorProxy activeProxy = _cluster.getAvailableMember(true, remainingTime);
                updateActiveProxy(activeProxy);
                if (activeProxy == null) {
                    String timeoutErrorMessage = _cluster.generateTimeoutErrorMessage(initialFailureTime, request);
                    if (_logger.isLoggable(Level.SEVERE))
                        _logger.log(Level.SEVERE, timeoutErrorMessage);
                    request.setRemoteOperationExecutionError(new RemoteException(timeoutErrorMessage));
                }
                return activeProxy;
            } catch (RemoteOperationRouterException e) {
                request.setRemoteOperationExecutionError(e);
                return null;
            }
        }
    }

    public RemoteOperationsExecutorProxy findActiveMemberUninterruptibly(RemoteOperationRequest<?> request, long initialFailureTime, RemoteOperationsExecutorProxy proxy) {
        try {
            return findActiveMember(request, initialFailureTime, proxy);
        } catch (InterruptedException e) {
            request.setRemoteOperationExecutionError(e);
            return null;
        }
    }

    public RemoteOperationsExecutorProxy findAnyAvailableMember(boolean activeOnly) {
        RemoteOperationsExecutorProxy activeProxy = getCandidate(null);
        if (RemoteOperationsExecutorProxy.isAvailable(activeProxy, activeOnly))
            return activeProxy;

        try {
            RemoteOperationsExecutorProxy result;
            long timeout = _cluster.getConfig().getActiveServerLookupTimeout();
            if (activeOnly) {
                synchronized (_lock) {
                    result = _cluster.getAvailableMember(true, timeout);
                    updateActiveProxy(result);
                }
            } else {
                result = _cluster.getAvailableMember(false, timeout);
            }
            return result;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        } catch (RemoteOperationRouterException e) {
            return null;
        }
    }

    public void onMemberConnected(RemoteOperationsExecutorProxy connectedMember) {
    }

    public void onMemberDisconnected(String disconnectedMemberName) {
    }

    /**
     * This method MUST be called from within a synchronized block.
     */
    protected abstract void updateActiveProxy(RemoteOperationsExecutorProxy newActiveProxy);
}

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

package com.gigaspaces.lrmi;


import com.gigaspaces.exception.lrmi.ApplicationException;
import com.gigaspaces.exception.lrmi.ProtocolException;
import com.gigaspaces.internal.lrmi.LRMIProxyMonitoringDetailsImpl;
import com.gigaspaces.lrmi.nio.async.FutureContext;
import com.gigaspaces.lrmi.nio.async.LRMIFuture;

/**
 * Invocation Handler for a Dynamic Proxy wrapping a Connection Pool.
 *
 * @author Igor Goldenberg
 * @since 4.0
 **/
@com.gigaspaces.api.InternalApi
public class ConnPoolInvocationHandler
        implements ClientPeerInvocationHandler {
    /**
     * cache dynamic proxy methods info and client peer id
     */
    private final ConnectionPool _connPool;

    public ConnPoolInvocationHandler(ConnectionPool connPool) {
        _connPool = connPool;
    }

    /**
     * Remote invocation via dedicated connection provided by ConnectionPool. NOTE: toString(),
     * hashCode(), equals() should be catch by upper layer of SmartStub.
     **/
    public Object invoke(Object proxy, LRMIMethod lrmiMethod, Object[] args)
            throws Throwable {
        ConnectionResource clientPeer = null;

        //States whether the connection should be freed when this invocation is complete
        boolean freeConnection = true;

        // in case of async method do not throw exception when failed to connect, instead return the value using future.
        // no need to free the connection in this case since the connection allocation was failed.
        try {
            clientPeer = _connPool.getConnection(lrmiMethod);
        } catch (Exception e) {
            if (lrmiMethod.isAsync) {
                ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
                LRMIFuture result = (LRMIFuture) FutureContext.getFutureResult();
                if (result == null) {
                    result = new LRMIFuture(contextClassLoader);
                } else {
                    result.reset(contextClassLoader);
                }
                FutureContext.setFutureResult(result);
                result.setResult(e);
                return null;
            } else {
                throw e;
            }
        }
        try {

            //If invocation is async, it will be released by the client handler once the invocation and its response cycle is complete
            freeConnection = !lrmiMethod.isAsync;
            return clientPeer.invoke(proxy, lrmiMethod, args, _connPool);
        } catch (ProtocolException ex) {
            throw ex.getCause();
        } catch (ApplicationException ex) {
            throw ex.getCause();
        } finally {
            if (clientPeer != null && freeConnection)
                _connPool.freeConnection(clientPeer);
        }

    }

    public long getGeneratedTraffic() {
        return _connPool.getGeneratedTraffic();
    }

    public long getReceivedTraffic() {
        return _connPool.getReceivedTraffic();
    }

    public void disable() {
        _connPool.disable();
    }

    public void enable() {
        _connPool.enable();
    }

    @Override
    public void close() {
        _connPool.close();
    }

    @Override
    public LRMIProxyMonitoringDetailsImpl getMonitoringDetails() {
        return _connPool.getMonitoringDetails();
    }

}
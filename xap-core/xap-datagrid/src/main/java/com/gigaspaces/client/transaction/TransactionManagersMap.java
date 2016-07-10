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

package com.gigaspaces.client.transaction;

import com.gigaspaces.internal.client.spaceproxy.ISpaceProxy;
import com.j_spaces.core.IJSpace;

import net.jini.core.transaction.server.TransactionManager;

import java.lang.ref.WeakReference;
import java.util.Hashtable;

/**
 * A transaction managers Map which maps a SpaceProxy instance to its equivalent transaction
 * manager.
 *
 * @author idan
 * @since 8.0
 */
@com.gigaspaces.api.InternalApi
public class TransactionManagersMap {

    private final Hashtable<ProxyComparator, WeakReference<TransactionManager>> _transactionManagersMap;

    public TransactionManagersMap() {
        _transactionManagersMap = new Hashtable<ProxyComparator, WeakReference<TransactionManager>>();
    }

    public TransactionManager get(IJSpace space) {
        WeakReference<TransactionManager> weakTxnManager = _transactionManagersMap.get(new ProxyComparator(space));
        return weakTxnManager == null ? null : weakTxnManager.get();
    }

    public void put(IJSpace space, TransactionManager transactionManager) {
        _transactionManagersMap.put(new ProxyComparator(space),
                new WeakReference<TransactionManager>(transactionManager));
    }

    public void remove(IJSpace space) {
        _transactionManagersMap.remove(new ProxyComparator(space));
    }

    /**
     * This class wraps IJSpace instance. It overrides equals() and hashcode() methods. It's used as
     * a key in m_LtmInstanceTable.
     */
    private final static class ProxyComparator {
        /**
         * proxy instance.
         */
        private final WeakReference<IJSpace> weakProxyReference;

        /**
         * Constructor.
         */
        ProxyComparator(IJSpace proxy) {
            weakProxyReference = new WeakReference<IJSpace>(proxy);
        }

        /**
         * Return the space proxy that this manager is connected to. May return <code>null</code> if
         * this reference object has been cleared by the garbage.
         *
         * @return space proxy.
         */
        private IJSpace getProxy() {
            return weakProxyReference.get();
        }

        /**
         * overrides equals() method of Object.
         */
        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof ProxyComparator)) {
                return false;
            }
            IJSpace objProxy = ((ProxyComparator) obj).getProxy();
            IJSpace thisProxy = getProxy();

            //may have been gc'd (weak ref.)
            if (thisProxy == null || objProxy == null) {
                //DON'T DO THIS: m_LtmInstanceTable.remove( this); - will cause infinite loop.
                return false;
            }

            if (((ISpaceProxy) objProxy).isClustered()) {
                return (thisProxy == objProxy);
            }
            return thisProxy.equals(objProxy);
        }

        /**
         * overrides hashcode() method of Object.
         */
        @Override
        public int hashCode() {
            IJSpace thisProxy = getProxy();
            if (thisProxy == null)
                return -1;

            return thisProxy.hashCode();
        }
    }


}

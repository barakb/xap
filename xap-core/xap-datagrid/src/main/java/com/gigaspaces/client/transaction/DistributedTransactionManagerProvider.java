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

import com.sun.jini.admin.DestroyAdmin;
import com.sun.jini.mahalo.TxnManager;

import net.jini.admin.Administrable;
import net.jini.core.transaction.TransactionException;
import net.jini.core.transaction.server.TransactionManager;

import java.rmi.RemoteException;

/**
 * A provider for a distributed Jini (embedded) transaction manager.
 *
 * @author idan
 * @since 8.0
 */
@com.gigaspaces.api.InternalApi
public class DistributedTransactionManagerProvider
        implements ITransactionManagerProvider {

    private static TransactionManager _transactionManager;
    private static int _txnManagerReferencesCount = 0;
    private static Object _lock = new Object();
    private boolean _destroyed = false;

    public DistributedTransactionManagerProvider() throws TransactionException {
        synchronized (_lock) {
            if (_transactionManager == null) {
                try {
                    TxnManager impl = MahaloFactory.createMahalo();
                    _transactionManager = impl.getLocalProxy();
                    _txnManagerReferencesCount = 0;
                } catch (Exception e) {
                    throw new TransactionException(e.getMessage(), e);
                }
            }
            _txnManagerReferencesCount++;
        }
    }

    @Override
    public void destroy() throws RemoteException {

        synchronized (_lock) {
            if (_destroyed)
                return;

            _destroyed = true;

            if (--_txnManagerReferencesCount == 0) {
                Administrable admin = (Administrable) _transactionManager;
                if (admin == null)
                    return;
                _transactionManager = null;
                Object adminObject = admin.getAdmin();
                if (adminObject instanceof DestroyAdmin) {
                    ((DestroyAdmin) adminObject).destroy();
                }
            }
        }
    }

    @Override
    public TransactionManager getTransactionManager() {
        if (_transactionManager == null) {
            //Force memory barrier, is transaction manager wasn't visible to this thread, it should be now, it should not be null
            //unless destroyed
            synchronized (_lock) {
                if (_destroyed)
                    throw new IllegalStateException("Transaction provider was already destroyed");
            }
        }
        return _transactionManager;
    }

    @Override
    protected void finalize() throws Throwable {
        try {
            destroy();
        } finally {
            super.finalize();
        }
    }

}

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

import com.gigaspaces.lrmi.ILRMIProxy;
import com.j_spaces.core.client.FinderException;
import com.j_spaces.core.client.LookupFinder;
import com.j_spaces.core.client.LookupRequest;

import net.jini.core.transaction.TransactionException;
import net.jini.core.transaction.server.TransactionManager;

import java.rmi.RemoteException;


/**
 * A provider for a lookup distributed Jini transaction manager.
 *
 * @author idan
 * @since 8.0
 */
@com.gigaspaces.api.InternalApi
public class LookupDistributedTransactionManagerProvider
        implements ITransactionManagerProvider {

    private final TransactionManager _transactionManager;

    public LookupDistributedTransactionManagerProvider(LookupTransactionManagerConfiguration config)
            throws TransactionException {
        try {
            LookupRequest request = LookupRequest.TransactionManager()
                    .setServiceName(config.getLookupTransactionName())
                    .setLocators(config.getLookupTransactionLocators())
                    .setGroups(config.getLookupTransactionGroups())
                    .setTimeout(config.getLookupTransactionTimeout());
            _transactionManager = (TransactionManager) LookupFinder.find(request);
        } catch (FinderException e) {
            throw new TransactionException(e.getMessage(), e);
        }
    }

    @Override
    public void destroy() throws RemoteException {
        if (_transactionManager instanceof ILRMIProxy)
            ((ILRMIProxy) _transactionManager).closeProxy();
    }

    @Override
    public TransactionManager getTransactionManager() {
        return _transactionManager;
    }

}

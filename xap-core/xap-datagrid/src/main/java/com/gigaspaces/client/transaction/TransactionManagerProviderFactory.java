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

import com.j_spaces.core.IJSpace;

import net.jini.core.transaction.TransactionException;

import java.rmi.RemoteException;


/**
 * A transaction manager factory used for creating a local/distributed/lookup transaction manager
 * provider.
 *
 * @author idan
 * @since 8.0
 */
@com.gigaspaces.api.InternalApi
public class TransactionManagerProviderFactory {

    public static ITransactionManagerProvider newInstance(IJSpace space, TransactionManagerConfiguration transactionManagerConfiguration)
            throws TransactionException, RemoteException {
        switch (transactionManagerConfiguration.getTransactionManagerType()) {
            case DISTRIBUTED:
                return new DistributedTransactionManagerProvider();
            case LOOKUP_DISTRIBUTED:
                return new LookupDistributedTransactionManagerProvider((LookupTransactionManagerConfiguration) transactionManagerConfiguration);
        }
        return null;
    }

}

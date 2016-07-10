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

import net.jini.core.transaction.server.TransactionManager;

import java.rmi.RemoteException;

/**
 * Interace for a transaction manager provider.
 *
 * @author idan
 * @since 8.0
 */
public interface ITransactionManagerProvider {

    /**
     * TransactionManager types.
     *
     * @author idan
     * @since 8.0
     */
    public enum TransactionManagerType {
        /**
         * Distributed Jini transaction manager (embedded).
         */
        DISTRIBUTED("distributed"),
        /**
         * Lookup (remote) distributed Jini transaction manager.
         */
        LOOKUP_DISTRIBUTED("lookup_distributed");

        private final String _nameInConfiguration;

        TransactionManagerType(String nameInConfiguration) {
            _nameInConfiguration = nameInConfiguration;
        }

        public String getNameInConfiguration() {
            return _nameInConfiguration;
        }

        public static TransactionManagerType getValue(String transactionManagerType) {
            for (TransactionManagerType type : TransactionManagerType.values()) {
                if (type._nameInConfiguration.compareTo(transactionManagerType) == 0)
                    return type;
            }
            return null;
        }

    }

    /**
     * Gets the provider's associated transaction manager.
     */
    public TransactionManager getTransactionManager();

    /**
     * Destroys the provider's associated transaction manager.
     */
    public void destroy() throws RemoteException;

}

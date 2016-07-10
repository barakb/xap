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

import net.jini.core.transaction.server.TransactionManager;

import org.openspaces.pu.service.ServiceDetails;
import org.springframework.util.Assert;

/**
 * Springs transaction manager ({@link org.springframework.transaction.PlatformTransactionManager}
 * using directly injected Jini {@link TransactionManager}. This transaction manager is mostly used
 * with applications that obtain the Jini transaction manager by other means than the ones provided
 * by {@link LocalJiniTransactionManager} and {@link LookupJiniTransactionManager}.
 *
 * @author kimchy
 */
public class DirectJiniTransactionManager extends AbstractJiniTransactionManager {

    private static final long serialVersionUID = -8773176073029897135L;

    private TransactionManager transactionManager;

    /**
     * Sets the Jini {@link TransactionManager} to be used. This is a required property.
     */
    public void setTransactionManager(TransactionManager transactionManager) {
        this.transactionManager = transactionManager;
    }

    /**
     * Returns the {@link TransactionManager} provided using the {@link
     * #setTransactionManager(TransactionManager)}.
     */
    @Override
    protected TransactionManager doCreateTransactionManager() throws Exception {
        Assert.notNull(transactionManager, "transactionManager is required property");
        return this.transactionManager;
    }

    public ServiceDetails[] getServicesDetails() {
        return new ServiceDetails[0];
    }
}

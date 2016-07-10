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

import org.openspaces.core.jini.JiniServiceFactoryBean;
import org.openspaces.pu.service.PlainServiceDetails;
import org.openspaces.pu.service.ServiceDetails;
import org.openspaces.pu.service.ServiceDetailsProvider;
import org.springframework.transaction.TransactionSystemException;

import java.util.Arrays;

/**
 * Springs transaction manager ({@link org.springframework.transaction.PlatformTransactionManager}
 * using Jini in order to lookup the transaction manager based on a name (can have <code>null</code>
 * value).
 *
 * <p>Uses {@link JiniServiceFactoryBean} in order to perform the lookup based on the specified
 * {@link #setTransactionManagerName(String)} and {@link #setLookupTimeout(Long)}. This usually
 * works with Jini Mahalo transaction manager.
 *
 * @author kimchy
 */
public class LookupJiniTransactionManager extends AbstractJiniTransactionManager implements ServiceDetailsProvider {

    private static final long serialVersionUID = -917940171952237730L;

    private String transactionManagerName;

    private Long lookupTimeout;

    private String[] groups;

    private String[] locators;

    /**
     * Sets the transaction manager name to perform the lookup by. Note, this is not the transaction
     * manager bean name, but an optional mahalo transaction manager name under which it was
     * started. Usually this is set to <code>null</code>.
     */
    public void setTransactionManagerName(String transactionManagerName) {
        this.transactionManagerName = transactionManagerName;
    }

    /**
     * Sets the lookupTimeout for the transaction manager lookup operation.
     */
    public void setLookupTimeout(Long lookupTimeout) {
        this.lookupTimeout = lookupTimeout;
    }

    /**
     * Sets the groups that will be used to look up the Jini transaction manager. Default to ALL
     * groups.
     */
    public void setGroups(String[] groups) {
        this.groups = groups;
    }

    /**
     * Sets specific locators to find the jini transaction manger.
     */
    public void setLocators(String[] locators) {
        this.locators = locators;
    }

    /**
     * Returns a Jini {@link TransactionManager} that is lookup up using {@link
     * JiniServiceFactoryBean}. The lookup can use a specified {@link #setTransactionManagerName(String)}
     * and a {@link #setLookupTimeout(Long)}.
     */
    @Override
    protected TransactionManager doCreateTransactionManager() throws Exception {
        JiniServiceFactoryBean serviceFactory = new JiniServiceFactoryBean();
        serviceFactory.setServiceClass(TransactionManager.class);
        serviceFactory.setServiceName(transactionManagerName);
        if (lookupTimeout != null) {
            serviceFactory.setTimeout(lookupTimeout);
        }
        if (groups != null) {
            serviceFactory.setGroups(groups);
        }
        if (locators != null) {
            serviceFactory.setLocators(locators);
        }
        serviceFactory.afterPropertiesSet();
        TransactionManager transactionManager = (TransactionManager) serviceFactory.getObject();
        if (transactionManager == null) {
            String groups = "ALL";
            if (this.groups != null) {
                groups = Arrays.asList(this.groups).toString();
            }
            throw new TransactionSystemException("Failed to find Jini transaction manager using groups [" + groups
                    + "], locators [" + Arrays.toString(locators) + "] timeout [" + lookupTimeout + "] and name [" + transactionManagerName + "]");
        }
        return transactionManager;
    }

    public ServiceDetails[] getServicesDetails() {
        StringBuilder longDesc = new StringBuilder();
        longDesc.append("Lookup ");
        if (groups != null) {
            longDesc.append(Arrays.toString(groups));
        }
        if (locators != null) {
            longDesc.append(Arrays.toString(locators));
        }
        return new ServiceDetails[]{new PlainServiceDetails(getBeanName(), SERVICE_TYPE, "lookup", getBeanName(), longDesc.toString())};
    }

    @Override
    protected boolean suppportsCustomIsolationLevel() {
        return true;
    }
}

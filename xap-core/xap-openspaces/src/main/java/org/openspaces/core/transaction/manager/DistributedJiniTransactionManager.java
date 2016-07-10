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

import com.gigaspaces.client.transaction.MahaloFactory;
import com.sun.jini.admin.DestroyAdmin;
import com.sun.jini.mahalo.TxnManager;

import net.jini.admin.Administrable;
import net.jini.core.transaction.server.TransactionManager;

import org.openspaces.pu.service.PlainServiceDetails;
import org.openspaces.pu.service.ServiceDetails;
import org.springframework.beans.factory.DisposableBean;

/**
 * A transaction manager that starts an embedded distributed mahalo transaction manager. This
 * transaction manager can be used to perform operations that span several Spaces or several
 * partitions within a Space.
 *
 * @author kimchy
 */
public class DistributedJiniTransactionManager extends AbstractJiniTransactionManager implements DisposableBean {

    private static final long serialVersionUID = 5766281396691553487L;

    private TxnManager impl;

    private TransactionManager proxy;

    @Override
    protected TransactionManager doCreateTransactionManager() throws Exception {
        impl = MahaloFactory.createMahalo();
        proxy = impl.getLocalProxy();
        return proxy;
    }

    @Override
    public void destroy() throws Exception {
        super.destroy();
        if (proxy == null) {
            return;
        }
        try {
            Object adminObject = ((Administrable) proxy).getAdmin();
            if (adminObject instanceof DestroyAdmin) {
                ((DestroyAdmin) adminObject).destroy();
            }
        } finally {
            proxy = null;
            impl = null;
        }
    }

    public ServiceDetails[] getServicesDetails() {
        return new ServiceDetails[]{new PlainServiceDetails(getBeanName(), SERVICE_TYPE, "distributed", getBeanName(), "Distributed (embedded)")};
    }

    @Override
    protected boolean suppportsCustomIsolationLevel() {
        return true;
    }
}

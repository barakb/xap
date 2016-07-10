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

package com.j_spaces.jdbc;

import com.gigaspaces.lrmi.RemoteStub;

import net.jini.core.transaction.Transaction;

import java.rmi.RemoteException;
import java.sql.SQLException;

/**
 * Smart stub for the {@link QueryProcessor}
 *
 * @author anna
 * @since 6.0
 */
@com.gigaspaces.api.InternalApi
public class QueryProcessorStub extends RemoteStub<IQueryProcessor> implements IQueryProcessor {
    static final long serialVersionUID = 6324495631092751521L;

    // HERE just for externalizable
    public QueryProcessorStub() {
    }

    public QueryProcessorStub(IQueryProcessor directObjRef, IQueryProcessor dynamicProxy) {
        super(directObjRef, dynamicProxy);
    }

    public void closeConnection(ConnectionContext context) throws RemoteException {
        getProxy().closeConnection(context);
    }

    public void setTransaction(Transaction transaction) throws RemoteException {
        getProxy().setTransaction(transaction);
    }

    public ResponsePacket executeQuery(RequestPacket request, ConnectionContext context)
            throws RemoteException, SQLException {
        return getProxy().executeQuery(request, context);
    }

    public boolean isAvailable() throws RemoteException {
        return getProxy().isAvailable();
    }

    public ConnectionContext newConnection() throws RemoteException {
        return getProxy().newConnection();
    }

    public QuerySession getSession(ConnectionContext context) throws RemoteException {
        return getProxy().getSession(context);
    }
}

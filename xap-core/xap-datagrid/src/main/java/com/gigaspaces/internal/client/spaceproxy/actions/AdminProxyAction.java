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

package com.gigaspaces.internal.client.spaceproxy.actions;

import com.gigaspaces.async.AsyncFuture;
import com.gigaspaces.async.AsyncFutureListener;
import com.gigaspaces.executor.SpaceTask;
import com.gigaspaces.internal.client.spaceproxy.ISpaceProxy;
import com.j_spaces.core.DropClassException;
import com.j_spaces.core.SpaceHealthStatus;

import net.jini.core.transaction.Transaction;
import net.jini.core.transaction.TransactionException;

import java.rmi.RemoteException;

public abstract class AdminProxyAction<TSpaceProxy extends ISpaceProxy> {
    public abstract void dropClass(TSpaceProxy spaceProxy, String className)
            throws RemoteException, DropClassException;

    public abstract void ping(TSpaceProxy spaceProxy)
            throws RemoteException;

    public abstract SpaceHealthStatus getSpaceHealthStatus(TSpaceProxy spaceProxy)
            throws RemoteException;

    public abstract AsyncFuture execute(TSpaceProxy spaceProxy, SpaceTask task, Object routing, Transaction txn, AsyncFutureListener listener)
            throws RemoteException, TransactionException;
}

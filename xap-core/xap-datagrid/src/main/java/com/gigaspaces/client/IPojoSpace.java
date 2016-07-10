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


package com.gigaspaces.client;

import com.gigaspaces.query.ISpaceQuery;
import com.j_spaces.core.LeaseContext;

import net.jini.core.entry.UnusableEntryException;
import net.jini.core.transaction.Transaction;
import net.jini.core.transaction.TransactionException;

import java.rmi.RemoteException;

/**
 * @author Guy Korland
 * @since 6.1
 * @deprecated Since 8.0 - Use {@link org.openspaces.core.GigaSpace} instead.
 */
@Deprecated
public interface IPojoSpace {
    /**
     * @deprecated Since 8.0 - Use {@link org.openspaces.core.GigaSpace} instead.
     */
    @Deprecated
    int count(Object template, Transaction transaction) throws UnusableEntryException, TransactionException, RemoteException;

    /**
     * @deprecated Since 8.0 - Use {@link org.openspaces.core.GigaSpace} instead.
     */
    @Deprecated
    int count(Object template, Transaction transaction, int modifiers) throws UnusableEntryException, TransactionException, RemoteException;

    /**
     * @deprecated Since 8.0 - Use {@link org.openspaces.core.GigaSpace} instead.
     */
    @Deprecated
    void clear(Object template, Transaction transaction) throws RemoteException, TransactionException, UnusableEntryException;

    /**
     * @deprecated Since 8.0 - Use {@link org.openspaces.core.GigaSpace} instead.
     */
    @Deprecated
    int clear(Object template, Transaction transaction, int modifiers) throws java.rmi.RemoteException, TransactionException, UnusableEntryException;

    /**
     * @deprecated Since 8.0 - Use {@link org.openspaces.core.GigaSpace} instead.
     */
    @Deprecated
    public Object read(Object template, Transaction transaction, long timeout) throws UnusableEntryException, TransactionException, InterruptedException, RemoteException;

    /**
     * @deprecated Since 8.0 - Use {@link org.openspaces.core.GigaSpace} instead.
     */
    @Deprecated
    public Object read(Object template, Transaction transaction, long timeout, int modifiers) throws UnusableEntryException, TransactionException, InterruptedException, RemoteException;

    /**
     * @deprecated Since 8.0 - Use {@link org.openspaces.core.GigaSpace} instead.
     */
    @Deprecated
    public Object readIfExists(Object template, Transaction transaction, long timeout) throws UnusableEntryException, TransactionException, InterruptedException, RemoteException;

    /**
     * @deprecated Since 8.0 - Use {@link org.openspaces.core.GigaSpace} instead.
     */
    @Deprecated
    public Object readIfExists(Object template, Transaction transaction, long timeout, int modifiers) throws UnusableEntryException, TransactionException, InterruptedException, RemoteException;

    /**
     * @deprecated Since 8.0 - Use {@link org.openspaces.core.GigaSpace} instead.
     */
    @Deprecated
    Object[] readMultiple(Object template, Transaction transaction, int limit) throws TransactionException, UnusableEntryException, RemoteException;

    /**
     * @deprecated Since 8.0 - Use {@link org.openspaces.core.GigaSpace} instead.
     */
    @Deprecated
    Object[] readMultiple(Object template, Transaction transaction, int limit, int modifiers) throws TransactionException, UnusableEntryException, RemoteException;

    /**
     * @deprecated Since 8.0 - Use {@link org.openspaces.core.GigaSpace} instead.
     */
    @Deprecated
    public Object take(Object template, Transaction transaction, long timeout) throws UnusableEntryException, TransactionException, InterruptedException, RemoteException;

    /**
     * @deprecated Since 8.0 - Use {@link org.openspaces.core.GigaSpace} instead.
     */
    @Deprecated
    public Object take(Object template, Transaction transaction, long timeout, int modifiers) throws UnusableEntryException, TransactionException, InterruptedException, RemoteException;

    /**
     * @deprecated Since 8.0 - Use {@link org.openspaces.core.GigaSpace} instead.
     */
    @Deprecated
    public Object takeIfExists(Object template, Transaction transaction, long timeout) throws UnusableEntryException, TransactionException, InterruptedException, RemoteException;

    /**
     * @deprecated Since 8.0 - Use {@link org.openspaces.core.GigaSpace} instead.
     */
    @Deprecated
    Object[] takeMultiple(Object template, Transaction transaction, int limit) throws TransactionException, UnusableEntryException, RemoteException;

    /**
     * @deprecated Since 8.0 - Use {@link org.openspaces.core.GigaSpace} instead.
     */
    @Deprecated
    Object[] takeMultiple(Object template, Transaction txn, int limit, int modifiers) throws TransactionException, UnusableEntryException, RemoteException;

    /**
     * @deprecated Since 8.0 - Use {@link org.openspaces.core.GigaSpace} instead.
     */
    @Deprecated
    public <T> ISpaceQuery<T> snapshot(Object object) throws RemoteException;

    /**
     * @deprecated Since 8.0 - Use {@link org.openspaces.core.GigaSpace} instead.
     */
    @Deprecated
    public LeaseContext write(Object object, Transaction transaction, long lease) throws TransactionException, RemoteException;

    /**
     * @deprecated Since 8.0 - Use {@link org.openspaces.core.GigaSpace} instead.
     */
    @Deprecated
    public LeaseContext write(Object object, Transaction transaction, long lease, long timeout, int modifiers) throws TransactionException, RemoteException;

    /**
     * @deprecated Since 8.0 - Use {@link org.openspaces.core.GigaSpace} instead.
     */
    @Deprecated
    LeaseContext[] writeMultiple(Object[] objects, Transaction transaction, long lease) throws TransactionException, RemoteException;

    /**
     * @deprecated Since 8.0 - Use {@link org.openspaces.core.GigaSpace} instead.
     */
    @Deprecated
    LeaseContext[] writeMultiple(Object[] objects, Transaction transaction, long lease, int modifiers) throws TransactionException, RemoteException;

    /**
     * @deprecated Since 8.0 - Use {@link org.openspaces.core.GigaSpace} instead.
     */
    @Deprecated
    LeaseContext[] writeMultiple(Object[] objects, Transaction transaction, long[] leases, int modifiers) throws TransactionException, RemoteException;

    /**
     * @deprecated Since 8.0 - Use {@link org.openspaces.core.GigaSpace} instead.
     */
    @Deprecated
    public Object update(Object updatedEntry, Transaction transaction, long lease, long timeout) throws TransactionException, UnusableEntryException, RemoteException, InterruptedException;

    /**
     * @deprecated Since 8.0 - Use {@link org.openspaces.core.GigaSpace} instead.
     */
    @Deprecated
    public Object update(Object updatedEntry, Transaction transaction, long lease, long timeout, int modifiers) throws TransactionException, UnusableEntryException, RemoteException, InterruptedException;

    /**
     * @deprecated Since 8.0 - Use {@link org.openspaces.core.GigaSpace} instead.
     */
    @Deprecated
    public Object[] updateMultiple(Object[] entries, Transaction transaction, long[] leases) throws UnusableEntryException, TransactionException, RemoteException;

    /**
     * @deprecated Since 8.0 - Use {@link org.openspaces.core.GigaSpace} instead.
     */
    @Deprecated
    public Object[] updateMultiple(Object[] objects, Transaction transaction, long[] leases, int modifiers) throws UnusableEntryException, TransactionException, java.rmi.RemoteException;
}

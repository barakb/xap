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

package com.gigaspaces.internal.cluster.node.impl.handlers;

import com.gigaspaces.internal.cluster.node.IReplicationInContext;
import com.gigaspaces.internal.server.space.SpaceEngine;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.j_spaces.core.ExtendedAnswerHolder;
import com.j_spaces.core.UnknownTypeException;
import com.j_spaces.core.client.EntryAlreadyInSpaceException;
import com.j_spaces.core.client.EntryNotInSpaceException;
import com.j_spaces.core.client.Modifiers;
import com.j_spaces.core.client.UpdateModifiers;

import net.jini.core.entry.UnusableEntryException;
import net.jini.core.transaction.Transaction;
import net.jini.core.transaction.TransactionException;

import java.rmi.RemoteException;


@com.gigaspaces.api.InternalApi
public class PrimaryBackupSpaceReplicationEvictionProtectionEntryEventHandler
        extends PrimaryBackupSpaceReplicationEntryEventHandler {

    public PrimaryBackupSpaceReplicationEvictionProtectionEntryEventHandler(
            SpaceEngine engine, boolean isCentralAndExternalDB) {
        super(engine, isCentralAndExternalDB);
    }

    @Override
    protected ExtendedAnswerHolder executeUpdateOperation(
            IReplicationInContext context, Transaction txn,
            IEntryPacket entry, IEntryPacket previousEntry,
            boolean partialUpdate, boolean overrideVersion, long lease,
            boolean twoPhaseCommit, int updateModifiers)
            throws UnusableEntryException, UnknownTypeException,
            TransactionException, RemoteException, InterruptedException {

        if (twoPhaseCommit) {
            return executeUpdateOperationOnPrepareWithEvictionReplicationProtectionRequired(txn,
                    entry,
                    previousEntry,
                    lease,
                    updateModifiers,
                    partialUpdate);
        }

        return super.executeUpdateOperation(context,
                txn,
                entry,
                previousEntry,
                partialUpdate,
                overrideVersion,
                lease,
                twoPhaseCommit,
                updateModifiers);

    }

    private ExtendedAnswerHolder executeUpdateOperationOnPrepareWithEvictionReplicationProtectionRequired(
            Transaction txn, IEntryPacket entry, IEntryPacket previousEntry,
            long lease, int updateModifiers, boolean partialUpdate) throws TransactionException,
            UnusableEntryException, UnknownTypeException, RemoteException,
            InterruptedException, EntryNotInSpaceException {

        updateModifiers |= (partialUpdate ? UpdateModifiers.PARTIAL_UPDATE : Modifiers.NONE);

        while (true) {
            try {
                // write previous entry under no txn before performing update
                _engine.write(previousEntry,
                        null /* txn */,
                        previousEntry.getTTL() /* lease */,
                        UpdateModifiers.WRITE_ONLY /* modifiers */,
                        true /* from replication */,
                        false /* origin */,
                        null /* SpaceContext */);
            } catch (EntryAlreadyInSpaceException e) {
                // ok
            }

            try {
                return _engine.update(entry,
                        txn /* txn */,
                        lease /* lease */,
                        0 /* timeout */,
                        null /* SpaceContext */,
                        true /* fromReplication */,
                        false /* origin */,
                        false /* newRouter */,
                        UpdateModifiers.UPDATE_ONLY | updateModifiers);

            } catch (EntryNotInSpaceException e) {
                // previous entry has been evicted, re-write previous entry and retry
            }
        }

    }

}

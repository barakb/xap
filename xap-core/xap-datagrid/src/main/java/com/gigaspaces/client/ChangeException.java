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

import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.gigaspaces.lrmi.LRMIInvocationContext;

import java.io.IOException;
import java.util.Collection;

/**
 * Thrown when any error occurred when trying to perform change of entries
 *
 * @author eitany
 * @since 9.1
 */

public class ChangeException
        extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private Collection<ChangedEntryDetails<?>> _changedEntries;
    private int _numChangedEntries;
    private Collection<FailedChangedEntryDetails> _entriesFailedToChange;
    private Collection<Throwable> _errors;

    //Externalizable
    public ChangeException() {
    }


    public ChangeException(String message, Collection<ChangedEntryDetails<?>> changedEntries,
                           Collection<FailedChangedEntryDetails> entriesFailedToChange,
                           Collection<Throwable> errors) {
        super(message, initCause(errors, entriesFailedToChange));
        _changedEntries = changedEntries;
        _entriesFailedToChange = entriesFailedToChange;
        _errors = errors;
        _numChangedEntries = changedEntries.size();
    }

    public ChangeException(String message, int numChangedEntries,
                           Collection<FailedChangedEntryDetails> entriesFailedToChange,
                           Collection<Throwable> errors) {
        super(message, initCause(errors, entriesFailedToChange));
        _numChangedEntries = numChangedEntries;
        _entriesFailedToChange = entriesFailedToChange;
        _errors = errors;

    }


    private static Throwable initCause(Collection<Throwable> errors, Collection<FailedChangedEntryDetails> entriesFailedToChange) {
        if (errors.size() + entriesFailedToChange.size() != 1)
            return null;

        if (!errors.isEmpty())
            return errors.iterator().next();

        return entriesFailedToChange.iterator().next().getCause();
    }

    /**
     * Returns the successfully done changes.
     */
    public Collection<ChangedEntryDetails<?>> getSuccesfullChanges() {
        return _changedEntries;
    }


    /**
     * Returns the number of successfully changes.
     */
    public int getNumSuccesfullChanges() {
        return _numChangedEntries;
    }


    /**
     * Returns the failed changes.
     */
    public Collection<FailedChangedEntryDetails> getFailedChanges() {
        return _entriesFailedToChange;
    }

    /**
     * Return any general errors occurred which are not associated to a specific entry that were
     * being changed.
     */
    public Collection<Throwable> getErrors() {
        return _errors;
    }


    private void writeObject(java.io.ObjectOutputStream out)
            throws IOException {
        if (LRMIInvocationContext.getEndpointLogicalVersion().greaterOrEquals(PlatformLogicalVersion.v9_5_0)) {
            out.writeBoolean(_changedEntries != null);
            if (_changedEntries != null)
                IOUtils.writeObject(out, _changedEntries);
            else
                out.writeInt(_numChangedEntries);
        } else {
            IOUtils.writeObject(out, _changedEntries);
        }
        IOUtils.writeObject(out, _entriesFailedToChange);
        IOUtils.writeObject(out, _errors);
    }

    private void readObject(java.io.ObjectInputStream in)
            throws IOException, ClassNotFoundException {
        if (LRMIInvocationContext.getEndpointLogicalVersion().greaterOrEquals(PlatformLogicalVersion.v9_5_0)) {
            if (in.readBoolean()) {
                _changedEntries = IOUtils.readObject(in);
                _numChangedEntries = _changedEntries.size();
            } else
                _numChangedEntries = in.readInt();
        } else {
            _changedEntries = IOUtils.readObject(in);
        }
        _entriesFailedToChange = IOUtils.readObject(in);
        _errors = IOUtils.readObject(in);
    }

}

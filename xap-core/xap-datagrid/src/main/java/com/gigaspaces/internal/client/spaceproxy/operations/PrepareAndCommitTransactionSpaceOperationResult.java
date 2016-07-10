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

package com.gigaspaces.internal.client.spaceproxy.operations;

import com.gigaspaces.internal.utils.Textualizer;

import net.jini.core.transaction.TransactionException;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.rmi.RemoteException;

/**
 * @author eitany
 * @since 9.0
 */
@com.gigaspaces.api.InternalApi
public class PrepareAndCommitTransactionSpaceOperationResult extends SpaceOperationResult {
    private static final long serialVersionUID = 1L;

    private int _state;

    public PrepareAndCommitTransactionSpaceOperationResult() {
    }

    public void processExecutionException() throws TransactionException, RemoteException, InterruptedException {
        final Exception executionException = getExecutionException();
        if (executionException == null)
            return;
        if (executionException instanceof TransactionException)
            throw (TransactionException) executionException;
        if (executionException instanceof RemoteException)
            throw (RemoteException) executionException;
        if (executionException instanceof InterruptedException)
            throw (InterruptedException) executionException;

        onUnexpectedException(executionException);
    }

    public void setState(int state) {
        _state = state;
    }

    public int getState() {
        return _state;
    }

    @Override
    public void toText(Textualizer textualizer) {
        super.toText(textualizer);
        textualizer.append("state", _state);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeInt(_state);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        super.readExternal(in);
        _state = in.readInt();
    }

}

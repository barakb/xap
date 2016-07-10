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

import com.gigaspaces.async.AsyncResult;
import com.gigaspaces.async.AsyncResultFilter;
import com.gigaspaces.async.AsyncResultFilter.Decision;
import com.gigaspaces.async.AsyncResultFilterEvent;
import com.gigaspaces.async.AsyncResultsReducer;
import com.gigaspaces.async.internal.DefaultAsyncResult;
import com.gigaspaces.executor.SpaceTask;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.remoting.routing.partitioned.PartitionedClusterExecutionType;
import com.gigaspaces.internal.remoting.routing.partitioned.PartitionedClusterRemoteOperationRouter;
import com.gigaspaces.internal.server.space.operations.SpaceOperationsCodes;
import com.gigaspaces.internal.utils.Textualizer;

import net.jini.core.transaction.Transaction;
import net.jini.core.transaction.server.ServerTransaction;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Niv Ingberg
 * @since 9.0.0
 */
@com.gigaspaces.api.InternalApi
public class ExecuteTaskSpaceOperationRequest extends SpaceOperationRequest<ExecuteTaskSpaceOperationResult> {
    private static final long serialVersionUID = 1L;
    private static final AsyncResult<Object>[] EMPTY_RESULTS = new AsyncResult[0];

    private SpaceTask<?> _task;
    private Transaction _txn;
    private transient Object _routingValue;
    private transient AsyncResultsReducer<Object, Object> _reducer;
    private transient AsyncResultFilter<Object> _filter;

    private transient List<AsyncResult<Object>> _prevResults;
    private Transaction _originalTranscation;

    /**
     * Required for Externalizable
     */
    public ExecuteTaskSpaceOperationRequest() {
    }

    public ExecuteTaskSpaceOperationRequest(SpaceTask<?> task, Transaction txn, Object routingValue) {
        this._task = task;
        this._txn = txn;
        this._originalTranscation = txn;
        this._routingValue = routingValue;
        this._reducer = task instanceof AsyncResultsReducer ? (AsyncResultsReducer<Object, Object>) task : null;
        this._filter = task instanceof AsyncResultFilter ? (AsyncResultFilter<Object>) task : null;
    }

    @Override
    public void toText(Textualizer textualizer) {
        super.toText(textualizer);
        textualizer.append("task", _task);
    }

    public SpaceTask<?> getTask() {
        return _task;
    }

    @Override
    public Transaction getTransaction() {
        return _txn;
    }

    @Override
    public int getOperationCode() {
        return SpaceOperationsCodes.EXECUTE_TASK;
    }

    @Override
    public ExecuteTaskSpaceOperationResult createRemoteOperationResult() {
        return new ExecuteTaskSpaceOperationResult();
    }

    @Override
    public boolean isDedicatedPoolRequired() {
        return true;
    }

    @Override
    public PartitionedClusterExecutionType getPartitionedClusterExecutionType() {
        if (_routingValue == null && _reducer == null)
            throw new UnsupportedOperationException("Executing a non-distributed task without a routing value is not supported. Task class: " + _task.getClass().getName());
        return _routingValue == null ? PartitionedClusterExecutionType.BROADCAST_CONCURRENT : PartitionedClusterExecutionType.SINGLE;
    }

    @Override
    public Object getPartitionedClusterRoutingValue(PartitionedClusterRemoteOperationRouter router) {
        return _routingValue;
    }

    @Override
    public boolean processPartitionResult(ExecuteTaskSpaceOperationResult remoteOperationResult, List<ExecuteTaskSpaceOperationResult> previousResults,
                                          int numOfPartitions) {
        AsyncResult<Object> currResult = new DefaultAsyncResult<Object>(remoteOperationResult.getResult(), remoteOperationResult.getExecutionException());
        if (_filter != null)
            return filterResult(currResult, numOfPartitions);

        _prevResults = append(_prevResults, currResult);
        return true;
    }

    @Override
    public Object getAsyncFinalResult() throws Exception {
        ExecuteTaskSpaceOperationResult remoteResult = getRemoteOperationResult();
        if (remoteResult != null) {
            if (_reducer == null) {
                if (remoteResult.getExecutionException() != null)
                    throw remoteResult.getExecutionException();
                return remoteResult.getResult();
            }
            _prevResults = append(_prevResults, new DefaultAsyncResult<Object>(remoteResult.getResult(), remoteResult.getExecutionException()));
        }

        return _reducer.reduce(_prevResults);
    }

    @Override
    public boolean beforeOperationExecution(boolean isEmbedded) {
        if (!super.beforeOperationExecution(isEmbedded))
            return false;

        // Optimization: the following code is intended for remote execution only.
        if (isEmbedded)
            return true;

        if (_txn != null) {
            final ServerTransaction transaction = (ServerTransaction) _txn;
            try {
                // Required on embedded transaction manager so the transaction won't be joined in the server
                if (transaction.isEmbeddedMgrProxySideInstance() && transaction.needParticipantsJoin()) {
                    _txn = transaction.createCopy();
                    ((ServerTransaction) _txn).setEmbeddedMgrProxySideInstance(false);
                }
            } catch (RemoteException e) {
                setRemoteOperationExecutionError(new IllegalStateException("Caught remote exception on embedded transaction manager", e));
                return false;
            }
        }
        return true;
    }

    @Override
    public void afterOperationExecution(int partitionId) {
        super.afterOperationExecution(partitionId);
        _txn = _originalTranscation;
    }

    private boolean filterResult(AsyncResult<Object> currResult, int maxResults) {
        if (_prevResults == null)
            _prevResults = new ArrayList<AsyncResult<Object>>();
        AsyncResult<Object>[] prevResults = _prevResults.isEmpty() ? EMPTY_RESULTS : _prevResults.toArray(new AsyncResult[_prevResults.size()]);

        AsyncResultFilterEvent<Object> event = new AsyncResultFilterEvent<Object>(currResult, prevResults, maxResults);
        Decision filterResult = _filter.onResult(event);
        switch (filterResult) {
            case CONTINUE:
                _prevResults = append(_prevResults, currResult);
                return true;
            case SKIP:
                return true;
            case BREAK:
                _prevResults = append(_prevResults, currResult);
                return false;
            case SKIP_AND_BREAK:
                return false;
            default:
                // TOLOG LB: Log this illegal filterResult.
                return false;
        }
    }

    private static List<AsyncResult<Object>> append(List<AsyncResult<Object>> prevResults, AsyncResult<Object> result) {
        if (prevResults == null)
            prevResults = new ArrayList<AsyncResult<Object>>();
        prevResults.add(result);
        return prevResults;
    }

    @Override
    public String getLRMIMethodTrackingId() {
        return "executeTask";
    }

    @Override
    public void writeExternal(ObjectOutput out)
            throws IOException {
        super.writeExternal(out);

        IOUtils.writeObject(out, _task);
        IOUtils.writeWithCachedStubs(out, _txn);
    }

    @Override
    public void readExternal(ObjectInput in)
            throws IOException, ClassNotFoundException {
        super.readExternal(in);

        _task = IOUtils.readObject(in);
        _txn = IOUtils.readWithCachedStubs(in);
    }
}

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

package com.gigaspaces.internal.cluster.node.impl.replica;

import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationTargetGroup;
import com.gigaspaces.internal.cluster.node.impl.router.IReplicationMonitoredConnection;
import com.gigaspaces.internal.cluster.node.replica.ISpaceCopyResult;
import com.gigaspaces.internal.cluster.node.replica.ISpaceSynchronizeReplicaState;
import com.gigaspaces.internal.cluster.node.replica.ISpaceSynchronizeResult;
import com.gigaspaces.internal.cluster.node.replica.SpaceReplicaStage;
import com.gigaspaces.time.SystemTime;

import net.jini.space.InternalSpaceException;

import java.util.Collection;
import java.util.Vector;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


@com.gigaspaces.api.InternalApi
public class SpaceReplicaState
        implements ISpaceSynchronizeReplicaState {

    private final IReplicationMonitoredConnection _originConnection;
    private final Lock _lock = new ReentrantLock();
    private final Condition _condition = _lock.newCondition();
    private final boolean _isSynchronize;
    private final Collection<SpaceCopyReplicaRunnable> _consumers = new Vector<SpaceCopyReplicaRunnable>();
    private final long _progressTimeout;
    private final IReplicationTargetGroup _targetGroup;
    private final String _replicaSourceLookupName;
    private boolean _copyStageDone = false;
    private boolean _synchronizeDone = false;
    private volatile Exception _failureReason;
    private volatile SpaceReplicaStage _stage = SpaceReplicaStage.COPY;
    private volatile ISpaceCopyResult _copyResult;
    private volatile ISpaceSynchronizeResult _syncResult;

    public SpaceReplicaState(IReplicationMonitoredConnection originConnection,
                             boolean isSynchronize, long progressTimeout, IReplicationTargetGroup targetGroup) {
        _originConnection = originConnection;
        _isSynchronize = isSynchronize;
        _progressTimeout = progressTimeout;
        _replicaSourceLookupName = originConnection.getFinalEndpointLookupName();
        _targetGroup = targetGroup;
        if (_isSynchronize && _targetGroup != null) {
            _targetGroup.addSynchronizeState(_replicaSourceLookupName, this);
        }
    }

    public ISpaceCopyResult waitForCopyResult() throws InterruptedException {
        try {
            return waitForCopyResult(Long.MAX_VALUE, TimeUnit.DAYS);
        } catch (TimeoutException e) {
            throw new InternalSpaceException(e);
        }
    }

    public ISpaceCopyResult waitForCopyResult(long time, TimeUnit unit)
            throws InterruptedException, TimeoutException {
        _lock.lock();
        try {
            waitForCopyResultHelper(time, unit);
            return mergedResults();
        } finally {
            _lock.unlock();
        }
    }

    //Should be called under lock
    private void waitForCopyResultHelper(long time, TimeUnit unit)
            throws InterruptedException, TimeoutException {
        long lastActiviteTime = -1;
        long remainingTime = unit.toMillis(time);
        while (!isCopyDone()) {
            //Get last activity time
            long lastActiviteTime1 = lastActiviteTime;
            for (SpaceCopyReplicaRunnable consumer1 : _consumers)
                lastActiviteTime1 = Math.max(lastActiviteTime1, consumer1.getLastIterationTimeStamp());
            lastActiviteTime = lastActiviteTime1;

            //No progress, fail process
            if (SystemTime.timeMillis() - lastActiviteTime > _progressTimeout) {
                for (SpaceCopyReplicaRunnable consumer : _consumers)
                    consumer.abort();
                _failureReason = new ReplicaNoProgressException("No progress in replica stage for the past " + _progressTimeout + " milliseconds");
                break;
            }

            long waitTime = Math.min(_progressTimeout, remainingTime);
            remainingTime -= waitTime;

            if (!_condition.await(waitTime, TimeUnit.MILLISECONDS) && remainingTime <= 0)
                throw new TimeoutException();

        }
    }

    // Should be called under lock
    private ISpaceCopyResult mergedResults() {
        if (_copyResult != null)
            return _copyResult;

        ISpaceCopyIntermediateResult mergedResult = null;
        for (SpaceCopyReplicaRunnable consumer : _consumers) {
            ISpaceCopyIntermediateResult result = consumer.getIntermediateResult();
            if (mergedResult == null)
                mergedResult = result;
            else
                mergedResult = mergedResult.merge(result);
        }

        if (_failureReason != null)
            mergedResult = mergedResult.mergeFailure(_failureReason);

        _copyResult = mergedResult.toFinalResult();
        return _copyResult;
    }

    public boolean isDone() {
        _lock.lock();
        try {
            if (_failureReason != null)
                return true;
            final boolean copyDone = isCopyDone();
            return copyDone && (!_isSynchronize || _synchronizeDone);
        } finally {
            _lock.unlock();
        }
    }

    public boolean isFailed() {
        return _failureReason != null;
    }

    public void updateSynchronizationDone() {
        _lock.lock();
        try {
            _synchronizeDone = true;
            _stage = SpaceReplicaStage.DONE;
            _condition.signalAll();
        } finally {
            _lock.unlock();
        }

    }

    // Should be called under lock
    private boolean isCopyDone() {
        return _copyStageDone;
    }

    public ISpaceSynchronizeResult getSynchronizeResult() {
        return _syncResult;
    }

    public ISpaceSynchronizeResult waitForSynchronizeCompletion()
            throws InterruptedException {
        try {
            return waitForSynchronizeCompletion(Long.MAX_VALUE, TimeUnit.DAYS);
        } catch (TimeoutException e) {
            throw new InternalSpaceException(e);
        }
    }

    public ISpaceSynchronizeResult waitForSynchronizeCompletion(long time,
                                                                TimeUnit unit) throws InterruptedException, TimeoutException {
        _lock.lock();
        try {
            if (_syncResult != null)
                return _syncResult;


            long remainingTime = unit.toMillis(time);
            long copyStageTimeStamp = SystemTime.timeMillis();
            waitForCopyResultHelper(time, unit);
            remainingTime -= (SystemTime.timeMillis() - copyStageTimeStamp);

            long lastProgressTimeStamp = SystemTime.timeMillis();
            while (!isDone()) {
                //Get last activity time
                lastProgressTimeStamp = Math.max(_targetGroup.getLastProcessTimeStamp(_replicaSourceLookupName), lastProgressTimeStamp);

                //No progress, fail process
                if (SystemTime.timeMillis() - lastProgressTimeStamp > _progressTimeout) {
                    _failureReason = new ReplicaNoProgressException("No progress in synchronize stage for the past " + _progressTimeout + " milliseconds");
                    break;
                }

                long waitTime = Math.min(_progressTimeout, remainingTime);
                remainingTime -= waitTime;

                if (!_condition.await(waitTime, TimeUnit.MILLISECONDS) && remainingTime <= 0)
                    throw new TimeoutException();
            }

            _syncResult = new SpaceSynchronizeResult(_failureReason);
            return _syncResult;
        } finally {
            _lock.unlock();
        }
    }

    public SpaceReplicaStage getStage() {
        return _stage;
    }

    public ISpaceCopyResult getCopyResult() {
        _lock.lock();
        try {
            return _copyResult;
        } finally {
            _lock.unlock();
        }
    }

    public void addReplicateConsumer(SpaceCopyReplicaRunnable consumer) {
        _consumers.add(consumer);
    }

    @Override
    public void abort(long timeout, TimeUnit units) throws InterruptedException, TimeoutException {
        for (SpaceCopyReplicaRunnable consumer : _consumers) {
            //The timeout is not percise since one handler can consume all the time and then we
            //move to the next handler with new time, This can be fixed by updating the timeout each iteration but this is not that important
            consumer.abort();
        }
        waitForCopyResult(timeout, units);
    }

    public void signalEntireCopyStageDoneSucessfully() {
        _lock.lock();
        try {
            _copyStageDone = true;

            stopAllConsumers();

            if (_isSynchronize)
                _stage = SpaceReplicaStage.SYNCHRONIZING;
            else
                _stage = SpaceReplicaStage.DONE;
            _condition.signalAll();
            _originConnection.close();
        } finally {
            _lock.unlock();
        }

    }

    public void signalSingleCopyStageDone() {
        _lock.lock();
        try {
            for (SpaceCopyReplicaRunnable consumer : _consumers)
                consumer.getHandler().resumeNow();
        } finally {
            _lock.unlock();
        }
    }

    public void signalCopyStageFailed(Exception error) {
        _lock.lock();
        try {
            if (_failureReason != null)
                return;

            _failureReason = error;
            _copyStageDone = true;

            stopAllConsumers();

            _condition.signalAll();
            _originConnection.close();
        } finally {
            _lock.unlock();
        }
    }

    private void stopAllConsumers() {
        for (SpaceCopyReplicaRunnable consumer : _consumers)
            consumer.getHandler().stop(3, TimeUnit.SECONDS);
    }

}

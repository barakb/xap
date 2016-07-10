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

package com.gigaspaces.internal.cluster.node.impl;

import com.gigaspaces.internal.cluster.node.replica.ISpaceCopyResult;
import com.gigaspaces.internal.cluster.node.replica.ISpaceSynchronizeReplicaState;
import com.gigaspaces.internal.cluster.node.replica.ISpaceSynchronizeResult;
import com.gigaspaces.internal.cluster.node.replica.SpaceReplicaStage;
import com.j_spaces.core.SpaceCopyStatus;
import com.j_spaces.core.SpaceCopyStatusImpl;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


@com.gigaspaces.api.InternalApi
public class FailedSyncSpaceReplicateState
        implements ISpaceSynchronizeReplicaState, ISpaceCopyResult, ISpaceSynchronizeResult {
    private final Exception _failureReason;

    public static ISpaceSynchronizeReplicaState createFailedSyncState(Exception failureReason) {
        return new FailedSyncSpaceReplicateState(failureReason);
    }

    private FailedSyncSpaceReplicateState(Exception failureReason) {
        _failureReason = failureReason;
    }

    public boolean isDone() {
        return true;
    }

    public SpaceReplicaStage getStage() {
        return SpaceReplicaStage.COPY;
    }

    public ISpaceCopyResult waitForCopyResult() throws InterruptedException {
        return this;
    }

    public ISpaceCopyResult waitForCopyResult(long time, TimeUnit unit)
            throws InterruptedException, TimeoutException {
        return this;
    }

    public ISpaceSynchronizeResult getSynchronizeResult() {
        return this;
    }

    public ISpaceSynchronizeResult waitForSynchronizeCompletion()
            throws InterruptedException {
        return this;
    }

    public ISpaceSynchronizeResult waitForSynchronizeCompletion(long time,
                                                                TimeUnit unit) throws InterruptedException, TimeoutException {
        return this;
    }

    public Exception getFailureReason() {
        return _failureReason;
    }

    public boolean isFailed() {
        return true;
    }

    public boolean isSuccessful() {
        return false;
    }

    public ISpaceCopyResult getCopyResult() {
        return this;
    }

    public boolean isEmpty() {
        return true;
    }

    @Override
    public void abort(long timeout, TimeUnit units) {
    }

    public SpaceCopyStatus toOldResult(short operationType, String targetMember) {
        SpaceCopyStatusImpl result = new SpaceCopyStatusImpl(operationType, targetMember);
        result.setCauseException(getFailureReason());
        return result;
    }

    public String getStringDescription(String remoteSpaceMember, String remoteMemberUrl, String spaceName, boolean spaceSyncOperation, long duration) {
        final String operationType = spaceSyncOperation ? "recover" : "copy data";
        return "Failed to " + operationType + " from space [" + remoteMemberUrl + "]";
    }
}

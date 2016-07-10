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

package com.gigaspaces.internal.server.space.recovery.group;

import com.gigaspaces.internal.cluster.node.impl.replica.SpaceSynchronizeResult;
import com.gigaspaces.internal.cluster.node.replica.ISpaceCopyReplicaState;
import com.gigaspaces.internal.cluster.node.replica.ISpaceCopyResult;
import com.gigaspaces.internal.cluster.node.replica.ISpaceSynchronizeReplicaState;
import com.gigaspaces.internal.cluster.node.replica.ISpaceSynchronizeResult;
import com.gigaspaces.internal.cluster.node.replica.SpaceReplicaStage;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Wraps an {@link ISpaceCopyReplicaState} as a {@link ISpaceSynchronizeReplicaState} that the sync
 * is considered done after the copy stage
 *
 * @author eitany
 * @since 8.0
 */
@com.gigaspaces.api.InternalApi
public class CopyOnlySynchronizeSpaceReplicate
        implements ISpaceSynchronizeReplicaState {

    private final ISpaceCopyReplicaState _spaceCopyReplica;

    public CopyOnlySynchronizeSpaceReplicate(
            ISpaceCopyReplicaState spaceCopyReplica) {
        _spaceCopyReplica = spaceCopyReplica;
    }

    public ISpaceSynchronizeResult getSynchronizeResult() {
        return new SpaceSynchronizeResult(null);
    }

    public ISpaceSynchronizeResult waitForSynchronizeCompletion()
            throws InterruptedException {
        _spaceCopyReplica.waitForCopyResult();
        return new SpaceSynchronizeResult(null);
    }

    public ISpaceSynchronizeResult waitForSynchronizeCompletion(long time,
                                                                TimeUnit unit) throws InterruptedException,
            TimeoutException {
        _spaceCopyReplica.waitForCopyResult(time, unit);
        return new SpaceSynchronizeResult(null);
    }

    public ISpaceCopyResult getCopyResult() {
        return _spaceCopyReplica.getCopyResult();
    }

    public SpaceReplicaStage getStage() {
        return _spaceCopyReplica.getStage();
    }

    public boolean isDone() {
        return _spaceCopyReplica.isDone();
    }

    public ISpaceCopyResult waitForCopyResult() throws InterruptedException {
        return _spaceCopyReplica.waitForCopyResult();
    }

    public ISpaceCopyResult waitForCopyResult(long time, TimeUnit unit)
            throws InterruptedException, TimeoutException {
        return _spaceCopyReplica.waitForCopyResult(time, unit);
    }

    @Override
    public void abort(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException {
        _spaceCopyReplica.abort(timeout, unit);
    }

}

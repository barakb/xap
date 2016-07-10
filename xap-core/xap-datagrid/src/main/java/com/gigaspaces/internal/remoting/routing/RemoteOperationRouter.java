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

package com.gigaspaces.internal.remoting.routing;

import com.gigaspaces.async.AsyncFutureListener;
import com.gigaspaces.internal.remoting.RemoteOperationFutureListener;
import com.gigaspaces.internal.remoting.RemoteOperationRequest;
import com.gigaspaces.internal.remoting.RemoteOperationResult;
import com.gigaspaces.internal.remoting.routing.clustered.RemoteOperationsExecutorProxy;

import java.util.List;

/**
 * @author Niv Ingberg
 * @since 9.0.1
 */
public interface RemoteOperationRouter {
    <T extends RemoteOperationResult> void execute(RemoteOperationRequest<T> request) throws InterruptedException;

    <T extends RemoteOperationResult> RemoteOperationFutureListener<T> createFutureListener(RemoteOperationRequest<T> request, AsyncFutureListener<Object> listener);

    <T extends RemoteOperationResult> void executeAsync(RemoteOperationRequest<T> request, RemoteOperationFutureListener<T> futureListener);

    void executeOneway(RemoteOperationRequest<?> request) throws InterruptedException;

    RemoteOperationsExecutorProxy getCachedMember();

    RemoteOperationsExecutorProxy getAnyAvailableMember();

    RemoteOperationsExecutorProxy getAnyActiveMember();

    void getAllAvailableMembers(List<RemoteOperationsExecutorProxy> availableMembers);

    void close();


}

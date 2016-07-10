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

package org.openspaces.extensions;

import com.gigaspaces.async.AsyncFuture;
import com.gigaspaces.async.AsyncFutureListener;
import com.gigaspaces.async.AsyncResult;
import com.gigaspaces.client.ChangeModifiers;
import com.gigaspaces.client.ChangeResult;
import com.gigaspaces.client.ChangeSet;
import com.gigaspaces.client.ReadModifiers;
import com.gigaspaces.client.TakeModifiers;
import com.gigaspaces.query.ISpaceQuery;

import org.openspaces.core.GigaSpace;
import org.openspaces.core.executor.DistributedTask;
import org.openspaces.core.executor.Task;
import org.springframework.dao.DataAccessException;

import java.io.Serializable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * @author Barak Bar Orion
 * @since 10.1
 */
public class AsyncExtension {
    public static <T> CompletableFuture<T> asyncRead(GigaSpace gigaSpace, T template) throws DataAccessException {
        return toCompletableFuture(gigaSpace.asyncRead(template));
    }

    public static <T> CompletableFuture<T> asyncRead(GigaSpace gigaSpace, T template, long timeout, TimeUnit timeUnit) throws DataAccessException {
        return toCompletableFuture(gigaSpace.asyncRead(template, timeUnit.toMillis(timeout)));
    }

    public static <T> CompletableFuture<T> asyncRead(GigaSpace gigaSpace, T template, long timeout, TimeUnit timeUnit, ReadModifiers modifiers) throws DataAccessException {
        return toCompletableFuture(gigaSpace.asyncRead(template, timeUnit.toMillis(timeout), modifiers));
    }

    public static <T> CompletableFuture<T> asyncRead(GigaSpace gigaSpace, ISpaceQuery<T> template) throws DataAccessException {
        return toCompletableFuture(gigaSpace.asyncRead(template));
    }

    public static <T> CompletableFuture<T> asyncRead(GigaSpace gigaSpace, ISpaceQuery<T> template, long timeout, TimeUnit timeUnit) throws DataAccessException {
        return toCompletableFuture(gigaSpace.asyncRead(template, timeUnit.toMillis(timeout)));
    }

    public static <T> CompletableFuture<T> asyncRead(GigaSpace gigaSpace, ISpaceQuery<T> template, long timeout, TimeUnit timeUnit, ReadModifiers modifiers) throws DataAccessException {
        return toCompletableFuture(gigaSpace.asyncRead(template, timeUnit.toMillis(timeout), modifiers));
    }

    public static <T> CompletableFuture<T> asyncTake(GigaSpace gigaSpace, T template) throws DataAccessException {
        return toCompletableFuture(gigaSpace.asyncTake(template));
    }

    public static <T> CompletableFuture<T> asyncTake(GigaSpace gigaSpace, T template, long timeout, TimeUnit timeUnit) throws DataAccessException {
        return toCompletableFuture(gigaSpace.asyncTake(template, timeUnit.toMillis(timeout)));
    }

    public static <T> CompletableFuture<T> asyncTake(GigaSpace gigaSpace, T template, long timeout, TimeUnit timeUnit, TakeModifiers modifiers) throws DataAccessException {
        return toCompletableFuture(gigaSpace.asyncTake(template, timeUnit.toMillis(timeout), modifiers));
    }

    public static <T> CompletableFuture<T> asyncTake(GigaSpace gigaSpace, ISpaceQuery<T> template) throws DataAccessException {
        return toCompletableFuture(gigaSpace.asyncTake(template));
    }

    public static <T> CompletableFuture<T> asyncTake(GigaSpace gigaSpace, ISpaceQuery<T> template, long timeout, TimeUnit timeUnit) throws DataAccessException {
        return toCompletableFuture(gigaSpace.asyncTake(template, timeUnit.toMillis(timeout)));
    }

    public static <T> CompletableFuture<T> asyncTake(GigaSpace gigaSpace, ISpaceQuery<T> template, long timeout, TimeUnit timeUnit, TakeModifiers modifiers) throws DataAccessException {
        return toCompletableFuture(gigaSpace.asyncTake(template, timeUnit.toMillis(timeout), modifiers));
    }

    public static <T extends Serializable> CompletableFuture<T> execute(GigaSpace gigaSpace, Task<T> task) {
        return toCompletableFuture(gigaSpace.execute(task));
    }

    public static <T extends Serializable> CompletableFuture<T> execute(GigaSpace gigaSpace, Task<T> task, Object routing) {
        return toCompletableFuture(gigaSpace.execute(task, routing));
    }

    public static <T extends Serializable> CompletableFuture<T> execute(GigaSpace gigaSpace, Task<T> task, Object... routing) {
        return toCompletableFuture(gigaSpace.execute(task, routing));
    }

    public static <T extends Serializable, R> CompletableFuture<R> execute(GigaSpace gigaSpace, DistributedTask<T, R> task) {
        return toCompletableFuture(gigaSpace.execute(task));
    }

    public static <T> CompletableFuture<ChangeResult<T>> asyncChange(GigaSpace gigaSpace, ISpaceQuery<T> query, ChangeSet changeSet) {
        return toCompletableFuture(gigaSpace.asyncChange(query, changeSet));
    }

    public static <T> CompletableFuture<ChangeResult<T>> asyncChange(GigaSpace gigaSpace, ISpaceQuery<T> query, ChangeSet changeSet, long timeout, TimeUnit timeUnit) {
        return toCompletableFuture(gigaSpace.asyncChange(query, changeSet, timeUnit.toMillis(timeout)));
    }

    public static <T> CompletableFuture<ChangeResult<T>> asyncChange(GigaSpace gigaSpace, ISpaceQuery<T> query, ChangeSet changeSet, ChangeModifiers modifiers) {
        return toCompletableFuture(gigaSpace.asyncChange(query, changeSet, modifiers));
    }

    public static <T> CompletableFuture<ChangeResult<T>> asyncChange(GigaSpace gigaSpace, ISpaceQuery<T> query, ChangeSet changeSet, ChangeModifiers modifiers, long timeout, TimeUnit timeUnit) {
        return toCompletableFuture(gigaSpace.asyncChange(query, changeSet, modifiers, timeUnit.toMillis(timeout)));
    }

    public static <T> CompletableFuture<ChangeResult<T>> asyncChange(GigaSpace gigaSpace, T template, ChangeSet changeSet) {
        return toCompletableFuture(gigaSpace.asyncChange(template, changeSet));
    }

    public static <T> CompletableFuture<ChangeResult<T>> asyncChange(GigaSpace gigaSpace, T template, ChangeSet changeSet, long timeout, TimeUnit timeUnit) {
        return toCompletableFuture(gigaSpace.asyncChange(template, changeSet, timeUnit.toMillis(timeout)));
    }

    public static <T> CompletableFuture<ChangeResult<T>> asyncChange(GigaSpace gigaSpace, T template, ChangeSet changeSet, ChangeModifiers modifiers) {
        return toCompletableFuture(gigaSpace.asyncChange(template, changeSet, modifiers));
    }

    public static <T> CompletableFuture<ChangeResult<T>> asyncChange(GigaSpace gigaSpace, T template, ChangeSet changeSet, ChangeModifiers modifiers, long timeout, TimeUnit timeUnit) {
        return toCompletableFuture(gigaSpace.asyncChange(template, changeSet, modifiers, timeUnit.toMillis(timeout)));
    }


    private static <T> CompletableFuture<T> toCompletableFuture(AsyncFuture<T> asyncFuture) {
        final CompletableFuture<T> res = new CompletableFuture<T>();
        asyncFuture.setListener(new AsyncFutureListener<T>() {
            @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
            @Override
            public void onResult(AsyncResult<T> result) {
                if (result.getException() != null) {
                    res.completeExceptionally(result.getException());
                } else {
                    res.complete(result.getResult());
                }
            }
        });
        return res;
    }

    @SuppressWarnings("unchecked")
    private static <T> CompletableFuture<T> toCompletableFuture(Future<T> future) {
        return toCompletableFuture(((AsyncFuture) future));
    }
}

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


package com.gigaspaces.async;

import com.gigaspaces.async.internal.CompoundFuture;

/**
 * A factory for different types of special futures, such as compound ones.
 *
 * @author Assaf Ronen
 * @since 6.6
 */
public final class FutureFactory {
    private FutureFactory() {
    }

    @SuppressWarnings("unchecked")
    public static <T, R> AsyncFuture<R> create(AsyncFuture<T>[] futures, AsyncResultsReducer<T, R> reducer) {
        return new CompoundFuture(futures, reducer);
    }

    @SuppressWarnings("unchecked")
    public static <T, R> AsyncFuture<R> create(AsyncFuture<T>[] futures, AsyncResultsReducer<T, R> reducer, AsyncResultFilter<T> moderator) {
        return new CompoundFuture(futures, reducer, moderator);
    }
}

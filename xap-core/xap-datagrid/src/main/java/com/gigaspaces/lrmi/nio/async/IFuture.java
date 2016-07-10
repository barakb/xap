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

package com.gigaspaces.lrmi.nio.async;

import com.gigaspaces.async.AsyncFuture;

/**
 * An internal interface extending the {@link com.gigaspaces.async.AsyncFuture}. <b>Internally
 * used</b>.
 */
public interface IFuture<T> extends AsyncFuture<T> {

    /**
     * The result has no type since it can be an exception or the real result
     */
    void setResult(Object result);
}

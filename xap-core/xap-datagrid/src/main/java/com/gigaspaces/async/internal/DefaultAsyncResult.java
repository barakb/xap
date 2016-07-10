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

package com.gigaspaces.async.internal;

import com.gigaspaces.async.AsyncResult;
import com.gigaspaces.internal.utils.Textualizable;
import com.gigaspaces.internal.utils.Textualizer;

/**
 * Default container for async result
 *
 * @author eitany
 * @since 7.0
 */
@com.gigaspaces.api.InternalApi
public class DefaultAsyncResult<T> implements AsyncResult<T>, Textualizable {
    private final T _result;
    private final Exception _exception;

    public DefaultAsyncResult(T result, Exception exception) {
        this._result = result;
        this._exception = exception;
    }

    @Override
    public T getResult() {
        return _result;
    }

    @Override
    public Exception getException() {
        return _exception;
    }

    @Override
    public String toString() {
        return Textualizer.toString(this);
    }

    @Override
    public void toText(Textualizer textualizer) {
        textualizer.append("result", this._result);
        textualizer.append("exception", this._exception);
    }
}

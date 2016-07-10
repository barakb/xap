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


package org.openspaces.core.executor.support;

import com.gigaspaces.async.AsyncResult;
import com.gigaspaces.async.AsyncResultsReducer;

import org.openspaces.core.util.numbers.NumberHelper;
import org.openspaces.core.util.numbers.NumberHelperFactory;

import java.util.List;

/**
 * A default implementation of a reducer that return the minimum of <code>T</code>.
 *
 * @author kimchy
 */
public class MinReducer<T extends Number> implements AsyncResultsReducer<T, T> {

    private volatile boolean ignoreExceptions;

    private NumberHelper<T> reduceHelper;

    public MinReducer(Class<T> reduceType) throws IllegalArgumentException {
        reduceHelper = NumberHelperFactory.getNumberHelper(reduceType);
    }

    /**
     * Causes the reducer to ignore exceptions and just sum the results that succeeded.
     */
    public MinReducer ignoreExceptions() {
        this.ignoreExceptions = true;
        return this;
    }

    public T reduce(List<AsyncResult<T>> results) throws Exception {
        T candidate = null;
        for (AsyncResult<T> result : results) {
            if (result.getException() != null) {
                if (!ignoreExceptions) {
                    throw result.getException();
                }
            } else {
                if (candidate == null) {
                    candidate = result.getResult();
                } else {
                    if (reduceHelper.compare(result.getResult(), candidate) < 0) {
                        candidate = result.getResult();
                    }
                }
            }
        }
        if (candidate == null) {
            throw new NoResultsException("No results to calculate reduce operations even though ignoring exceptions");
        }
        return candidate;
    }
}
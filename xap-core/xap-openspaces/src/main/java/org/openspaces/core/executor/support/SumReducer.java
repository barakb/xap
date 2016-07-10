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
 * A default implementation of a reducer that sums all types <code>T</code> into a result
 * <code>R</code>.
 *
 * @author kimchy
 */
public class SumReducer<T extends Number, R extends Number> implements AsyncResultsReducer<T, R> {

    private volatile boolean ignoreExceptions;

    protected final NumberHelper<R> redeuceHelper;

    public SumReducer(Class<R> reduceType) throws IllegalArgumentException {
        redeuceHelper = NumberHelperFactory.getNumberHelper(reduceType);
    }

    /**
     * Causes the reducer to ignore exceptions and just sum the results that succeeded.
     */
    public SumReducer ignoreExceptions() {
        this.ignoreExceptions = true;
        return this;
    }

    public R reduce(List<AsyncResult<T>> results) throws Exception {
        R sum = redeuceHelper.ZERO();
        for (AsyncResult<T> result : results) {
            if (result.getException() != null) {
                if (ignoreExceptions) {
                    continue;
                } else {
                    throw result.getException();
                }
            }
            sum = redeuceHelper.add(sum, result.getResult());
        }
        return sum;
    }
}
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

import java.util.List;

/**
 * A default implementation of a reducer that averages all types <code>T</code> into a result
 * <code>R</code>.
 *
 * @author kimchy
 */
public class AvgReducer<T extends Number, R extends Number> extends SumReducer<T, R> {

    public AvgReducer(Class<R> reduceType) throws IllegalArgumentException {
        super(reduceType);
    }

    /**
     * Causes the reducer to ignore exceptions and just sum the results that succeeded.
     */
    public AvgReducer ignoreExceptions() {
        super.ignoreExceptions();
        return this;
    }

    public R reduce(List<AsyncResult<T>> results) throws Exception {
        R sum = super.reduce(results);
        int count = 0;
        for (AsyncResult<T> result : results) {
            if (result.getException() == null) {
                count++;
            } else throw result.getException();
        }
        if (count == 0) {
            throw new NoResultsException("No results to calculate average on");
        }
        return redeuceHelper.div(sum, count);
    }
}
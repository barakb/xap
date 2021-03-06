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


package com.gigaspaces.client;

import com.gigaspaces.internal.exceptions.BatchQueryException;
import com.j_spaces.core.multiple.query.QueryMultiplePartialFailureException;

import java.util.List;

/**
 * Thrown when a space takeMultiple operation fails.</b>
 *
 * <p>Thrown on: <ul> <li>Partial and complete failure. <li>Cluster/single space topologies.
 * <li>SQLQueries/Templates. </ul>
 *
 * At space level - the failure is fail-fast - once an operation on an object failed - exception is
 * returned immediately and no other objects are processed. <br><br>
 *
 * At cluster level - if one of operation target spaces fails, first completion of the operation on
 * other spaces is done. Then the aggregated exception is thrown. <br>
 *
 * <p>The exception contains: <ul> <li>An array of exceptions that caused it. One exception per each
 * space that failed. <li>An array of entries that were successfully taken. </ul>
 *
 * <p> <b>Replaced {@link QueryMultiplePartialFailureException}.</b>
 *
 * </pre>
 *
 * @author anna
 * @since 7.1
 */

public class TakeMultipleException extends BatchQueryException {
    private static final long serialVersionUID = 1L;

    public TakeMultipleException() {
        super();
    }

    public TakeMultipleException(List<?> results, List<Throwable> exceptions) {
        super(results, exceptions);
    }

    public TakeMultipleException(Object[] results, List<Throwable> causes) {
        super(results, causes);
    }

    public TakeMultipleException(Throwable cause) {
        super(cause);
    }

}

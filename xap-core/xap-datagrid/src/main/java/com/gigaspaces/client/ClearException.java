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
 * Thrown when a space clear operation fails.</b>
 *
 * <p>Thrown on: <ul> <li>Partial and complete failure. <li>Cluster/single space topologies.
 * <li>SQLQueries/Templates. </ul>
 *
 * <p>The exception contains: <ul> <li>An array of exceptions that caused it. One exception per each
 * space that failed. </ul>
 *
 * At space level - the failure is fail-fast - once an operation on an object failed - exception is
 * returned immediately and no other objects are processed. <br><br>
 *
 * At cluster level - if one of operation target spaces fails, first completion of the operation on
 * other spaces is done. Then the aggregated exception is thrown. <br> <p> <b>Replaced {@link
 * QueryMultiplePartialFailureException}.</b>
 *
 * </pre>
 *
 * @author anna
 * @since 7.1
 */

public class ClearException extends BatchQueryException {
    private static final long serialVersionUID = 1L;

    public ClearException() {
        super();
    }

    public ClearException(List<Throwable> exceptions) {
        super((List<?>) null, exceptions);
    }

    public ClearException(Throwable cause) {
        super(cause);
    }
}

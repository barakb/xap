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

package org.openspaces.core;

import com.gigaspaces.client.WriteMultipleException.IWriteResult;

import org.openspaces.core.exception.ExceptionTranslator;

/**
 * Thrown when writeMultiple operation fails.
 *
 * <p>Thrown on: <ul> <li>Partial and complete failure. <li>Cluster/single space topologies. </ul>
 *
 * <p>The exception contains an array of write results where each result in the array is either a
 * lease or an exception upon failure, the result index corresponds to the entry index in the array
 * of entries which are being written/updated.
 *
 * <p> <b>Replaced {@link WriteMultiplePartialFailureException}.</b>
 *
 * </pre>
 *
 * @author eitany
 * @since 7.1
 */
public class WriteMultipleException extends WriteMultiplePartialFailureException {
    private static final long serialVersionUID = 1L;

    public WriteMultipleException(com.gigaspaces.client.WriteMultipleException cause, ExceptionTranslator exceptionTranslator) {
        super(cause, exceptionTranslator);
    }

    @Override
    public IWriteResult[] getResults() {
        return super.getResults();
    }


}

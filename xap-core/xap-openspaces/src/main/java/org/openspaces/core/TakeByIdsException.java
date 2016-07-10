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

import com.gigaspaces.client.ReadTakeByIdResult;

import org.openspaces.core.exception.ExceptionTranslator;
import org.springframework.dao.InvalidDataAccessResourceUsageException;

/**
 * Thrown when takeByIds operation fails.
 *
 * <p>Thrown on: <ul> <li>Partial and complete failure. <li>Cluster/single space topologies. </ul>
 *
 * <p>The exception contains an array of ITakeByIdResult objects where each result in the array
 * contains either a read object or an exception upon failure. The result array index corresponds to
 * the ID index in the operation's supplied IDs array.
 *
 * @author idan
 * @since 7.1.1
 */
public class TakeByIdsException extends InvalidDataAccessResourceUsageException {

    private static final long serialVersionUID = 1L;
    private final TakeByIdResult[] _results;

    public TakeByIdsException(com.gigaspaces.client.TakeByIdsException cause, ExceptionTranslator exceptionTranslator) {
        super(cause.getMessage(), cause);
        _results = new TakeByIdResult[cause.getResults().length];
        for (int i = 0; i < _results.length; i++) {
            _results[i] = new TakeByIdResult(cause.getResults()[i], exceptionTranslator);
        }
    }

    /**
     * Returns the results contained in the exception.
     *
     * @return An array of TakeByIdResult objects.
     */
    public TakeByIdResult[] getResults() {
        return _results;
    }

    /**
     * Holds a TakeByIdsException result. The result contains the object's Id, the result type, the
     * read object and the thrown exception.
     *
     * @author idan
     * @since 7.1.1
     */
    public static class TakeByIdResult {

        /**
         * Determines the result type of a take by id operation result.
         *
         * @author idan
         * @since 7.1.1
         */
        public enum TakeByIdResultType {
            /**
             * Operation failed - result contains the exception that caused the failure.
             */
            ERROR,
            /**
             * Operation succeeded - result contains the object matching the corresponded Id.
             */
            OBJECT,
            /**
             * Operation succeeded - there's no object matching the corresponded Id.
             */
            NOT_FOUND
        }

        private final ReadTakeByIdResult _result;
        private final TakeByIdResultType _resultType;
        private final Throwable _error;

        protected TakeByIdResult(ReadTakeByIdResult result, ExceptionTranslator exceptionTranslator) {
            _result = result;
            if (_result.isError()) {
                _resultType = TakeByIdResultType.ERROR;
                _error = exceptionTranslator.translate(result.getError());
            } else {
                _resultType = (_result.getObject() == null) ? TakeByIdResultType.NOT_FOUND : TakeByIdResultType.OBJECT;
                _error = null;
            }
        }

        /**
         * @return On error returns the exception that occurred, otherwise null.
         */
        public Throwable getError() {
            return _error;
        }

        /**
         * @return The object Id this result is relevant for.
         */
        public Object getId() {
            return _result.getId();
        }

        /**
         * @return The read object for this result or null if no object was found or an exception
         * occurred.
         */
        public Object getObject() {
            return _result.getObject();
        }

        /**
         * @return True if an exception is associated with this result.
         */
        public boolean isError() {
            return _result.isError();
        }

        /**
         * @return This result's object type.
         */
        public TakeByIdResultType getResultType() {
            return _resultType;
        }
    }

}

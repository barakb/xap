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
import com.j_spaces.core.LeaseContext;

import net.jini.core.lease.Lease;

import org.openspaces.core.exception.ExceptionTranslator;
import org.springframework.dao.InvalidDataAccessResourceUsageException;

/**
 * This exception is thrown when write multiple is called and for some reason the insertion of some
 * of the entries fails. Wrpas {@link com.j_spaces.core.multiple.write.WriteMultiplePartialFailureException}.
 *
 * @author kimchy
 * @see com.j_spaces.core.multiple.write.WriteMultiplePartialFailureException
 * @see com.gigaspaces.client.WriteMultipleException.IWriteResult
 * @deprecated since 7.1. use {@link WriteMultipleException}
 */
@Deprecated
public class WriteMultiplePartialFailureException extends InvalidDataAccessResourceUsageException {

    private static final long serialVersionUID = 1L;

    private final IWriteResult[] results;

    public WriteMultiplePartialFailureException(com.gigaspaces.client.WriteMultipleException cause, ExceptionTranslator exceptionTranslator) {
        super(cause.getMessage(), cause);
        results = new IWriteResult[cause.getResults().length];
        for (int i = 0; i < results.length; i++) {
            IWriteResult result = cause.getResults()[i];
            if (result.isError()) {
                results[i] = new TranslatedWriteResult(result, exceptionTranslator);
            } else {
                results[i] = result;
            }
        }
    }

    /**
     * @return an array of IResult objects.
     */
    public IWriteResult[] getResults() {
        return results;
    }

    private static class TranslatedWriteResult implements IWriteResult {

        private static final long serialVersionUID = 1L;
        private final IWriteResult result;
        private final Throwable error;

        private TranslatedWriteResult(IWriteResult result, ExceptionTranslator exceptionTranslator) {
            this.result = result;
            Exception translatedException;
            try {
                translatedException = exceptionTranslator.translate(result.getError());
            } catch (Exception e) {
                translatedException = e;
            }
            this.error = translatedException;
        }

        public ResultType getResultType() {
            return result.getResultType();
        }

        public Throwable getError() {
            return error;
        }

        public Lease getLease() {
            return result.getLease();
        }

        public boolean isError() {
            return result.isError();
        }

        public LeaseContext<?> getLeaseContext() {
            return result.getLeaseContext();
        }
    }
}

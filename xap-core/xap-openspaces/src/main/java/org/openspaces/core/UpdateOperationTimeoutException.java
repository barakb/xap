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

import com.j_spaces.core.client.OperationTimeoutException;

import org.springframework.dao.CannotAcquireLockException;

/**
 * Thrown when a space update operation timeouts after waiting for a transactional proper matching
 * entry. Wraps {@link com.j_spaces.core.client.OperationTimeoutException}.
 *
 * <p><i>Note:</i> To preserve timeout semantics defined by JavaSpace API, this exception is thrown
 * <b>only</b> if timeout expires and a space operation is performed with the UPDATE_OR_WRITE
 * modifier.
 *
 * @author kimchy
 * @see com.j_spaces.core.client.UpdateModifiers#UPDATE_OR_WRITE
 */
public class UpdateOperationTimeoutException extends CannotAcquireLockException {

    private static final long serialVersionUID = 4041065346557282521L;

    public UpdateOperationTimeoutException(OperationTimeoutException e) {
        super(e.getMessage(), e);
    }
}

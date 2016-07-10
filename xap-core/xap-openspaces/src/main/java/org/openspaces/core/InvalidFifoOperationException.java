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

import com.j_spaces.core.FifoOperationException;

import org.springframework.dao.InvalidDataAccessResourceUsageException;

/**
 * A base class for invalid fifo operations exceptions. Wraps {@link
 * com.j_spaces.core.FifoOperationException}.
 *
 * @author kimchy
 */
public class InvalidFifoOperationException extends InvalidDataAccessResourceUsageException {

    private static final long serialVersionUID = -1795908876690221029L;

    public InvalidFifoOperationException(FifoOperationException e) {
        super(e.getMessage(), e);
    }
}

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


package com.j_spaces.core.client;

/**
 * <code>OperationTimeoutException</code> is thrown when a space operation timeouts after waiting
 * for a transactional proper matching entry. <p>
 *
 * @author moran
 * @version 1.0
 * @since 5.2
 */
public class OperationTimeoutException
        extends RuntimeException {
    /**
     * default serial version id
     */
    private static final long serialVersionUID = 1L;

    /**
     * The detail message is saved for later retrieval by the {@link #getMessage()} method
     */
    private final static String DETAILED_MESSAGE = "Timeout expired after waiting for a transactional proper matching entry";

    /**
     * Constructs an OperationTimeoutException with a default detailed message. The cause is not
     * initialized, and may subsequently be initialized by a call to initCause.
     */
    public OperationTimeoutException() {
        super(DETAILED_MESSAGE);
    }

    /**
     * override fillInStackTrace() and do nothing
     */
    @Override
    public Throwable fillInStackTrace() {
        return null;
    }
}

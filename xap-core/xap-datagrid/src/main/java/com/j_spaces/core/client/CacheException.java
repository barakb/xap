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

/*******************************************************************************
 * Copyright (c) 2010 GigaSpaces Technologies Ltd. All rights reserved
 *
 * The software source code is proprietary and confidential information of GigaSpaces. You may use
 * the software source code solely under the terms and limitations of The license agreement granted
 * to you by GigaSpaces.
 *******************************************************************************/

/**
 * Exception class that is thrown when any kind of Cache related exception happens. It is mostly
 * used to wrap JavaSpaces exceptions such as <code>RemoteException</code>,
 * <code>TransactionException</code>. This class extends <code>RuntimeException</code> since the
 * implementation of {@link com.j_spaces.map.IMap} is implementing <code>java.util.Map</code>
 * methods which does not throw Exception.
 */
public class CacheException extends RuntimeException {
    private static final long serialVersionUID = 4723365057528448995L;

    /**
     * Constructs a new cache exception with the specified detail message. The cause is not
     * initialized, and may subsequently be initialized by a call to {@link #initCause}.
     *
     * @param message the detail message. The detail message is saved for later retrieval by the
     *                {@link #getMessage()} method.
     */
    public CacheException(String message) {
        super(message);
    }

    /**
     * Constructs a new cache exception with the specified detail message and cause.  <p>Note that
     * the detail message associated with <code>cause</code> is <i>not</i> automatically
     * incorporated in this runtime exception's detail message.
     *
     * @param message the detail message (which is saved for later retrieval by the {@link
     *                #getMessage()} method).
     * @param cause   the cause (which is saved for later retrieval by the {@link #getCause()}
     *                method).  (A <tt>null</tt> value is permitted, and indicates that the cause is
     *                nonexistent or unknown.)
     */
    public CacheException(String message, Throwable cause) {
        super(message, cause);
    }
}

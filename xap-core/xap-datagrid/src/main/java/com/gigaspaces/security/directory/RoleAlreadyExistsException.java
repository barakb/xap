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


package com.gigaspaces.security.directory;

import com.gigaspaces.security.SecurityException;

/**
 * Thrown if an {@link RoleManager} implementation encounters a non unique user.
 *
 * @author Moran Avigdor
 * @see RoleManager
 * @since 7.0.1
 */

public class RoleAlreadyExistsException extends SecurityException {
    private static final long serialVersionUID = 1L;

    /**
     * Constructs a <code>RoleAlreadyExistsException</code> with no detail message.
     */
    public RoleAlreadyExistsException() {
        super();
    }

    /**
     * Constructs a <code>RoleAlreadyExistsException</code> with the specified detail message.
     *
     * @param s the detail message.
     */
    public RoleAlreadyExistsException(String s) {
        super(s);
    }

    /**
     * Creates a <code>RoleAlreadyExistsException</code> with the specified detail message and
     * cause.
     *
     * @param message the detail message (which is saved for later retrieval by the {@link
     *                #getMessage()} method).
     * @param cause   the cause (which is saved for later retrieval by the {@link #getCause()}
     *                method). (A <tt>null</tt> value is permitted, and indicates that the cause is
     *                nonexistent or unknown.)
     */
    public RoleAlreadyExistsException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Creates a <code>RoleAlreadyExistsException</code> with the specified cause and a detail
     * message of <tt>(cause==null ? null : cause.toString())</tt> (which typically contains the
     * class and detail message of <tt>cause</tt>).
     *
     * @param cause the cause (which is saved for later retrieval by the {@link #getCause()}
     *              method). (A <tt>null</tt> value is permitted, and indicates that the cause is
     *              nonexistent or unknown.)
     */
    public RoleAlreadyExistsException(Throwable cause) {
        super(cause);
    }
}

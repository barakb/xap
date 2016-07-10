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

import com.gigaspaces.security.AccessDeniedException;
import com.gigaspaces.security.SecurityManager;

/**
 * A <code>DirectoryAccessDeniedException</code> is thrown as a result of an access restriction when
 * calling {@link SecurityManager#createDirectoryManager(UserDetails)}. This is usually the case
 * when the security implementation relies on external tools to modify the directory. <p> Our
 * default implementation allows management of the directory using the UI (GigaSpaces Management
 * Center), which might be irrelevant for custom implementation. This exception will restrict access
 * to the directory from direct calls to this API.
 *
 * @author Moran Avigdor
 * @since 7.0.1
 */

public class DirectoryAccessDeniedException extends AccessDeniedException {

    private static final long serialVersionUID = 1L;

    /**
     * Constructs a <code>DirectoryAccessDeniedException</code> with the specified detail message.
     *
     * @param message the detail message.
     */
    public DirectoryAccessDeniedException(String message) {
        super(message);
    }

    /**
     * Creates a <code>DirectoryAccessDeniedException</code> with the specified detail message and
     * cause.
     *
     * @param message the detail message (which is saved for later retrieval by the {@link
     *                #getMessage()} method).
     * @param cause   the cause (which is saved for later retrieval by the {@link #getCause()}
     *                method). (A <tt>null</tt> value is permitted, and indicates that the cause is
     *                nonexistent or unknown.)
     */
    public DirectoryAccessDeniedException(String message, Throwable cause) {
        super(message, cause);
    }
}

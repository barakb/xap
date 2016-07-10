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
 * A FinderException is thrown if an attempt to find space or container fails.
 *
 * @author Igor Goldenberg
 * @version 2.5
 **/

public class FinderException extends Exception {
    private static final long serialVersionUID = 7377796253648844233L;

    /**
     * Constructs a <code>FinderException</code> with <code>null</code> as its error detail
     * message.
     */
    public FinderException() {
        super();
    }

    /**
     * Constructs a <code>FinderException</code> with the specified detail message. The string
     * <code>s</code> can be retrieved later by the <code>{@link java.lang.Throwable#getMessage}</code>
     * method of class <code>java.lang.Throwable</code>.
     *
     * @param message the detail message.
     */
    public FinderException(String message) {
        super(message);
    }


    /**
     * Constructs a new FinderException with the specified detail message and cause.
     *
     * @param message the detail message (which is saved for later retrieval by the {@link
     *                #getMessage()} method).
     * @param cause   the cause (which is saved for later retrieval by the {@link #getCause()}
     *                method).  (A <tt>null</tt> value is permitted, and indicates that the cause is
     *                nonexistent or unknown.)
     * @since 4.0
     **/
    public FinderException(String message, Throwable cause) {
        super(message, cause);
    }


    /**
     * Constructs a new FinderException with the specified  cause.
     *
     * @param cause the cause (which is saved for later retrieval by the {@link #getCause()}
     *              method).  (A <tt>null</tt> value is permitted, and indicates that the cause is
     *              nonexistent or unknown.)
     * @since 6.5
     **/
    public FinderException(Throwable cause) {
        super(cause);
    }
}

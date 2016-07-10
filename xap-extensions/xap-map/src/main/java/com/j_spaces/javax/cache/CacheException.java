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



/*
 * 
 * Created on 02/06/2005
 *
 */
package com.j_spaces.javax.cache;

/**
 * Description: CacheException is a generic exception, which indicates a cache error has occurred.
 *
 * @version 1.0
 * @since 5.0
 * @deprecated
 */
@Deprecated
public class CacheException extends Exception {
    private static final long serialVersionUID = -2697840573761879590L;

    /**
     * Constructs a new CacheException.
     */
    public CacheException() {
        super();
    }

    /**
     * Constructs a new CacheException with a message string.
     *
     * @param message the detail message
     */
    public CacheException(String message) {
        super(message);
    }

    /**
     * Constructs a CacheException with a message string, and a cause.
     *
     * @param message the detail message
     * @param cause   the cause (which is saved for later retrieval by the {@link #getCause()}
     *                method).  (A <tt>null</tt> value is permitted, and indicates that the cause is
     *                nonexistent or unknown.)
     */
    public CacheException(String message, Throwable cause) {
        super(message, cause);
    }
}

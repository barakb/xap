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
 * @(#)DestroyedFailedException.java 1.0   02/10/2000  13:44PM
 */

package com.j_spaces.core;

/**
 * A DestroyedFailedException is thrown if an attempt to destroy an existing space fails.
 *
 * @author Igor Goldenberg
 * @version 1.0
 **/

public class DestroyedFailedException extends Exception {
    private static final long serialVersionUID = -5676732467094189282L;

    /**
     * Constructs a <code>DestroyedFailedException</code> with <code>null</code> as its error detail
     * message.
     */
    public DestroyedFailedException() {
        super();
    }


    /**
     * Constructs a <code>DestroyedFailedException</code> with the specified detail message. The
     * string <code>s</code> can be retrieved later by the <code>{@link
     * java.lang.Throwable#getMessage}</code> method of class <code>java.lang.Throwable</code>.
     *
     * @param s the detail message.
     */
    public DestroyedFailedException(String s) {
        super(s);
    }

    public DestroyedFailedException(String message, Throwable cause) {
        super(message, cause);
    }
}

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
 * @(#)CreateException.java 1.0   28/09/2000  14:39PM
 */

package com.j_spaces.core;

/**
 * A CreateException is thrown if an attempt to create a new space fails.
 *
 * @author Igor Goldenberg
 * @version 1.0
 **/

public class CreateException extends Exception {
    private static final long serialVersionUID = 6977428147516172354L;

    /**
     * Constructs a <code>CreateException</code> with <code>null</code> as its error detail
     * message.
     */
    public CreateException() {
        super();
    }


    /**
     * Constructs a <code>CreateException</code> with the specified detail message. The string
     * <code>s</code> can be retrieved later by the <code>{@link java.lang.Throwable#getMessage}</code>
     * method of class <code>java.lang.Throwable</code>.
     *
     * @param s the detail message.
     */
    public CreateException(String s) {
        super(s);
    }

    public CreateException(String message, Throwable cause) {
        super(message, cause);
    }
}

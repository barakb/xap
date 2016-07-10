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
 * @(#)NoSuchNameException.java 1.0   02/10/2000  13:40PM
 */

package com.j_spaces.core;

/**
 * A NoSuchNameException is thrown when a lookup of an object by name is performed and the name is
 * not found.
 *
 * @author Igor Goldenberg
 * @version 1.0
 **/

public class NoSuchNameException extends Exception {
    private static final long serialVersionUID = 7912396776416071801L;


    /**
     * Constructs a <code>NoSuchNameException</code> with <code>null</code> as its error detail
     * message.
     */
    public NoSuchNameException() {
        super();
    }


    /**
     * Constructs a <code>NoSuchNameException</code> with the specified detail message. The string
     * <code>s</code> can be retrieved later by the <code>{@link java.lang.Throwable#getMessage}</code>
     * method of class <code>java.lang.Throwable</code>.
     *
     * @param s the detail message.
     */
    public NoSuchNameException(String s) {
        super(s);
    }
}

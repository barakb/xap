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
 * @(#)DetailedUnusableEntryException.java 1.0   07/11/2002  13:40PM
 */


package com.j_spaces.core;

import net.jini.core.entry.UnusableEntryException;

/**
 * A DetailedUnusableEntryException is thrown when class version compatibility error, the space
 * server contains an older version of class.
 *
 * @author Igor Goldenberg
 * @version 1.0
 **/

public class DetailedUnusableEntryException extends UnusableEntryException {
    private static final long serialVersionUID = -5232702856139712301L;

    /**
     * Constructs a <code>DetailedUnusableEntryException</code> with the specified detail message.
     * The string <code>s</code> can be retrieved later by the <code>{@link
     * java.lang.Throwable#getMessage}</code> method of class <code>java.lang.Throwable</code>.
     *
     * @param s the detail message.
     */
    public DetailedUnusableEntryException(String s) {
        super(new Throwable(s));
    }

    public String getMessage() {
        return nestedExceptions[0].getMessage();
    }
}

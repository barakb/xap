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
 * @(#)SpaceCleanedException.java 1.0   09/12/2003  12:32:02
 */

package com.j_spaces.core.exception;

/**
 * This exception is thrown in case where there is an attempt to perform another space operation
 * such as write, read or take while "clean" is still in progress.
 *
 * @author Igor Goldenberg
 * @version 3.2
 **/

public class SpaceCleanedException extends SpaceUnavailableException {
    private static final long serialVersionUID = 6080728928804933215L;

    /**
     * Constructs a <code>SpaceCleanedException</code> with the specified detail message.
     *
     * @param s - the detail message
     */
    public SpaceCleanedException(String spaceName, String s) {
        super(spaceName, s);
    }
}
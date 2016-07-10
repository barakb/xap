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
 * @(#)DropClassException.java 1.0   08.08.2004  20:07:25
 */

package com.j_spaces.core;

/**
 * Thrown when drop class operation failed.
 *
 * @author Igor Goldenberg
 * @version 4.0
 **/

public class DropClassException extends Exception {
    private static final long serialVersionUID = 778986075236691110L;

    private String _className;

    public DropClassException(String msg, String className) {
        super(msg);
        _className = className;
    }

    /**
     * Returns desired dropped class.
     **/
    public String getClassName() {
        return _className;
    }
}

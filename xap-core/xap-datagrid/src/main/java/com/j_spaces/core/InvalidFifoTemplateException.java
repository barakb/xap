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
 * @(#)InvalidFifoTemplateException.java 1.0   19.05.2004  13:41:02
 */

package com.j_spaces.core;

/**
 * This exception is thrown if read or take operations executed in FIFO mode, but the template class
 * FIFO mode already been set to non FIFO.
 *
 * @author Igor Goldenberg
 * @version 4.0
 **/

public class InvalidFifoTemplateException extends FifoOperationException {
    private static final long serialVersionUID = 1L;

    private String _templateClassName;

    public InvalidFifoTemplateException(String templateClassName) {
        super("Failed to perform FIFO operation: class " + templateClassName + " metadata is not set to support FIFO.");
        this._templateClassName = templateClassName;
    }

    /**
     * Returns invalid template className.
     *
     * @return Invalid template className.
     **/
    public String getTemplateClassName() {
        return _templateClassName;
    }
}
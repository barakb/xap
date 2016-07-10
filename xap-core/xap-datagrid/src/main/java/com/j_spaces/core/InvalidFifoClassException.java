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
 * @(#)InvalidFifoClassException.java 1.0   19.05.2004  13:34:02
 */

package com.j_spaces.core;

/**
 * This exception used to be thrown during write operation when the Entry's class FIFO mode already
 * been defined and a later write operation define different FIFO mode. Since FIFO is now supported
 * per-operation, this validation is no longer relevant.
 *
 * @author Igor Goldenberg
 * @version 4.0
 * @see InvalidFifoTemplateException
 * @deprecated This exception is no longer thrown.
 **/
@Deprecated

public class InvalidFifoClassException extends FifoOperationException {
    private static final long serialVersionUID = 9179298086649918646L;

    final private String className;
    final private boolean entryFifoMode;
    final private boolean serverFifoMode;

    public InvalidFifoClassException(String className, boolean entryFifoMode, boolean serverFifoMode) {
        super("Entry was introduced with a non-matching FIFO order."
                + "\n\tRejected attempt to set the class " + className + " with FIFO " + entryFifoMode
                + "\n\t[ Class FIFO-type is known as " + serverFifoMode + " ]");

        this.className = className;
        this.entryFifoMode = entryFifoMode;
        this.serverFifoMode = serverFifoMode;
    }

    /**
     * Return invalid className.
     *
     * @return Returns invalid className.
     **/
    public String getClassName() {
        return className;
    }

    /**
     * Returns <code>true</code> if this class defined as FIFO, otherwise <code>false</code>.
     *
     * @return Returns <code>true</code> if this class defined as FIFO, otherwise
     * <code>false</code>.
     **/
    public boolean isFifoClass() {
        return serverFifoMode;
    }

    /**
     * Returns <code>true</code> if this entry defined as FIFO, otherwise <code>false</code>.
     *
     * @return Returns <code>true</code> if this entry defined as FIFO, otherwise
     * <code>false</code>.
     **/
    public boolean isEntryFifoMode() {
        return entryFifoMode;
    }
}
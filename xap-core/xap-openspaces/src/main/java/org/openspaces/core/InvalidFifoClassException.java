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


package org.openspaces.core;

import com.j_spaces.core.InvalidFifoTemplateException;

/**
 * This exception used to be thrown during write operation when the Entry's class FIFO mode already
 * been defined and a later write operation define different FIFO mode. Since FIFO is now supported
 * per-operation, this validation is no longer relevant. Wraps {@link
 * com.j_spaces.core.InvalidFifoClassException}.
 *
 * @author kimchy
 * @see InvalidFifoTemplateException
 * @deprecated This exception is no longer thrown.
 */
@Deprecated
public class InvalidFifoClassException extends InvalidFifoOperationException {

    private static final long serialVersionUID = 1523791345410332721L;

    private com.j_spaces.core.InvalidFifoClassException e;

    public InvalidFifoClassException(com.j_spaces.core.InvalidFifoClassException e) {
        super(e);
        this.e = e;
    }

    /**
     * Return invalid className.
     *
     * @see com.j_spaces.core.InvalidFifoClassException#getClassName()
     */
    public String getClassName() {
        return e.getClassName();
    }

    /**
     * Returns <code>true</code> if this class defined as FIFO, otherwise <code>false</code>.
     *
     * @see com.j_spaces.core.InvalidFifoClassException#isFifoClass()
     */
    public boolean isFifoClass() {
        return e.isFifoClass();
    }

}

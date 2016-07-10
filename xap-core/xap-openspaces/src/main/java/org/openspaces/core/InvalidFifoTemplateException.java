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

/**
 * This exception is thrown if read or take operations executed in FIFO mode, but the template class
 * FIFO mode already been set to non FIFO. Wraps {@link com.j_spaces.core.InvalidFifoTemplateException}.
 *
 * @author kimchy
 */
public class InvalidFifoTemplateException extends InvalidFifoOperationException {

    private static final long serialVersionUID = -8375088583591987356L;

    private com.j_spaces.core.InvalidFifoTemplateException e;

    public InvalidFifoTemplateException(com.j_spaces.core.InvalidFifoTemplateException e) {
        super(e);
        this.e = e;
    }

    /**
     * Returns invalid template className.
     *
     * @see com.j_spaces.core.InvalidFifoTemplateException#getTemplateClassName()
     */
    public String getTemplateClassName() {
        return e.getTemplateClassName();
    }

}

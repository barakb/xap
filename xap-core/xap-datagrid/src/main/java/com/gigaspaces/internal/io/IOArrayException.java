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

package com.gigaspaces.internal.io;

import java.io.IOException;

/**
 * This exception is thrown when an array serialization/deserialization fails on an IOException. It
 * contains the index of the item which caused the exception in addition to the original cause.
 *
 * @author Niv Ingberg
 * @since 7.1.1
 */
@com.gigaspaces.api.InternalApi
public class IOArrayException extends IOException {
    private static final long serialVersionUID = 1L;

    private int _index;

    public IOArrayException() {
        super();
    }

    public IOArrayException(int index) {
        super();
        this._index = index;
    }

    public IOArrayException(int index, String message) {
        super(message);
        this._index = index;
    }

    public IOArrayException(int index, String message, IOException cause) {
        this(index, message);
        initCause(cause);
    }

    public int getIndex() {
        return _index;
    }
}

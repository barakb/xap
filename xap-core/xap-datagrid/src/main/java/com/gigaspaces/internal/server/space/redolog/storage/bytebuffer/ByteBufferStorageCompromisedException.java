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

package com.gigaspaces.internal.server.space.redolog.storage.bytebuffer;

import com.gigaspaces.internal.server.space.redolog.IRedoLogFile;
import com.gigaspaces.internal.server.space.redolog.RedoLogFileCompromisedException;

/**
 * Thrown when a {@link ByteBufferRedoLogFileStorage} integrity is compromised, such as unavailable
 * storage or some error which causes a corruption of the redolog {@link
 * IRedoLogFile#validateIntegrity()}
 *
 * @author eitany
 * @since 7.1.1
 */
@com.gigaspaces.api.InternalApi
public class ByteBufferStorageCompromisedException
        extends RedoLogFileCompromisedException {

    public ByteBufferStorageCompromisedException(Throwable cause) {
        super(cause);
    }

    public ByteBufferStorageCompromisedException(String message) {
        super(message);
    }

    public ByteBufferStorageCompromisedException(String message, Exception cause) {
        super(message, cause);
    }

    /** */
    private static final long serialVersionUID = 1L;

}

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


package com.gigaspaces.client;

/**
 * Thrown when readByIds operation fails.
 *
 * <p>The exception contains an array of ReadTakeByIdResult objects where each result in the array
 * contains either a read object or an exception upon failure. The result array index corresponds to
 * the ID index in the operation's supplied IDs array.
 *
 * @author idan
 * @since 7.1.1
 */

public class ReadByIdsException extends ReadTakeByIdsException {

    /**
     *
     */
    private static final long serialVersionUID = 1L;

    /**
     * Default constructor required by Externalizable.
     */
    public ReadByIdsException() {
    }

    public ReadByIdsException(ReadTakeByIdResult[] results) {
        super(results);
    }

    public ReadByIdsException(Object[] ids, Object[] results, Exception[] exceptions) {
        super(ids, results, exceptions);
    }

}

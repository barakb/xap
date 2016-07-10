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

package com.j_spaces.core.cache;

/**
 * This exception is thrown by an iterator constructor when there is no sufficient data in the
 * database to create the iterator. The catcher of this exception should return <code>null</code> to
 * its caller.
 */
@com.gigaspaces.api.InternalApi
public class NullIteratorException extends Exception {
    private static final long serialVersionUID = 8401123769349985777L;

    public NullIteratorException() {
    }
}

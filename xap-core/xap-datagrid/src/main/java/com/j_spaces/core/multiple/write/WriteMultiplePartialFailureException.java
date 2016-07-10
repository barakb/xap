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

package com.j_spaces.core.multiple.write;

import com.gigaspaces.client.WriteMultipleException;

/**
 * WriteMultiplePartialFailureException is thrown when write multiple is called and for some reason
 * the insertion of some of the entries was failed.
 *
 * @see com.j_spaces.core.UnknownTypeException
 * @see net.jini.core.entry.UnusableEntryException
 * @since 6.5
 * @deprecated since 7.1. use {@link WriteMultipleException}
 */
@Deprecated
@com.gigaspaces.api.InternalApi
public class WriteMultiplePartialFailureException extends RuntimeException {
    private static final long serialVersionUID = -6396162967502290947L;

    protected WriteMultipleException.IWriteResult[] _results;

    public WriteMultiplePartialFailureException() {
    }

    /**
     * @return the results that are kept by this object.
     */
    public com.gigaspaces.client.WriteMultipleException.IWriteResult[] getResults() {
        return _results;
    }
}
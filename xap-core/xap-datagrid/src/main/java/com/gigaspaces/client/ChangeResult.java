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

import java.io.Serializable;
import java.util.Collection;


/**
 * Result of a change operation.
 *
 * @author Niv Ingberg
 * @since 9.1
 */
public interface ChangeResult<T>
        extends Serializable {
    /**
     * Returns a collection of {@link ChangedEntryDetails} of the changed entries. <note>This is
     * only supported if the {@link ChangeModifiers#RETURN_DETAILED_RESULTS} modifier was used,
     * otherwise this method will throw unsupported operation exception.
     *
     * @throws UnsupportedOperationException if the corresponding change operation was not used with
     *                                       the {@link ChangeModifiers#RETURN_DETAILED_RESULTS}
     *                                       modifier.
     */
    Collection<ChangedEntryDetails<T>> getResults();

    /**
     * Returns the number of changed entries
     */
    int getNumberOfChangedEntries();
}

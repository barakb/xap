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
import java.util.List;

/**
 * The result of a change operation executed on an entry
 *
 * @author Niv Ingberg
 * @since 9.1
 */
public interface ChangedEntryDetails<T> extends Serializable {
    /**
     * Returns the changed entry type name.
     */
    String getTypeName();

    /**
     * Returns the id of the changed entry.
     */
    Object getId();

    /**
     * Returns the version of the entry after the change opertion.
     */
    int getVersion();

    /**
     * Returns the results of the change operations which were inflicted on the changed entry. The
     * order of the list will be correlated to the order of operation which were specified on the
     * {@link ChangeSet} that was used for this change operation.
     *
     * @since 9.7
     */
    List<ChangeOperationResult> getChangeOperationsResults();
}

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


package org.openspaces.persistency.support;

import com.gigaspaces.datasource.DataIterator;

/**
 * A marker interface on top of the data source {@link com.gigaspaces.datasource.DataIterator} which
 * handles multiple data iterators.
 *
 * <p>Allows to get the underlying iterators. Note, this operation contract should only be called
 * before any iteration has been perfomed on any of the internal iterators.
 *
 * @author kimchy
 */
public interface MultiDataIterator extends DataIterator {

    /**
     * Returns the underlying iterators. Note, calling this method should only be performed if no
     * iteration has happened.
     */
    DataIterator[] iterators();
}

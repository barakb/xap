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

package org.openspaces.persistency.cassandra.datasource;

import com.gigaspaces.datasource.DataIterator;

/**
 * A {@link DataIterator} for single entry or empty result sets.
 *
 * @author Dan Kilman
 * @since 9.1.1
 */
public class SingleEntryDataIterator
        implements DataIterator<Object> {

    private Object entry;

    public SingleEntryDataIterator(Object entry) {
        this.entry = entry;
    }

    @Override
    public boolean hasNext() {
        return entry != null;
    }

    @Override
    public Object next() {
        Object result = entry;
        if (entry != null) {
            entry = null;
        }

        return result;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("remove is not supported");
    }

    @Override
    public void close() {
        // do nothing
    }
}

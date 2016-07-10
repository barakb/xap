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

/**
 *
 */
package com.j_spaces.sadapter.datasource;

import com.gigaspaces.datasource.DataIterator;
import com.gigaspaces.internal.transport.IEntryPacket;

import net.jini.space.InternalSpaceException;

/**
 * {@link EntryAdapterIterator} provides a wrapper for the {@link DataIterator} returned by the
 * {@link DataProvider} . {@link EntryAdapterIterator} converts the data objects returned by the
 * iterator to space internal representation.
 *
 * @author anna
 */
@com.gigaspaces.api.InternalApi
public class EntryAdapterIterator
        implements DataIterator<IEntryPacket> {
    // The data iterator
    protected DataIterator<Object> _iterator;
    // EntryAdapter that handles the conversion
    protected EntryAdapter _entryAdapter;

    /**
     * @param iterator
     */
    public EntryAdapterIterator(DataIterator<Object> iterator, EntryAdapter entryAdapter) {
        _iterator = iterator;
        _entryAdapter = entryAdapter;
    }

    /**
     * @see com.gigaspaces.datasource.DataIterator#close()
     */
    public void close() {
        _iterator.close();
    }

    /**
     * @return true is next iterator is exists , else false
     * @see java.util.Iterator#hasNext()
     */
    public boolean hasNext() {
        return _iterator.hasNext();
    }

    /**
     * @return IEntryPacket
     * @see java.util.Iterator#next()
     */
    public IEntryPacket next() {
        try {
            Object dataObject = _iterator.next();
            return _entryAdapter.toEntry(dataObject);
        } catch (Exception e) {
            throw new InternalSpaceException("Failed to retrieve next entry packet.", e);
        }
    }

    /**
     * @see java.util.Iterator#remove()
     */
    public void remove() {
        _iterator.remove();
    }

}

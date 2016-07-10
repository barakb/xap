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
 * A simple implementation wrapping several iterators and exposing them as a single (serial)
 * iterator.
 *
 * <p>Note, this implementation assumes that {@link #hasNext()} is called before {@link #next()} is
 * called. And that {@link #next()} is called only when {@link #hasNext()} returns
 * <code>true</code>.
 *
 * @author kimchy
 */
public class SerialMultiDataIterator implements MultiDataIterator {

    final private DataIterator[] iterators;

    private int currentIterator;

    public SerialMultiDataIterator(DataIterator... iterators) {
        this.iterators = iterators;
        currentIterator = 0;
    }

    public DataIterator[] iterators() {
        return iterators;
    }

    public boolean hasNext() {
        for (; currentIterator < iterators.length; currentIterator++) {
            if (iterators[currentIterator].hasNext())
                return true;
        }

        return false;
    }

    public Object next() {
        return iterators[currentIterator].next();
    }

    public void close() {
        for (DataIterator iterator : iterators) {
            iterator.close();
        }
    }

    public void remove() {
        throw new UnsupportedOperationException("remove is not supported in multi iterator");
    }
}

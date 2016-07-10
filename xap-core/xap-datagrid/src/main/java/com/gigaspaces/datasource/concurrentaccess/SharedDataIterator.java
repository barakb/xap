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

package com.gigaspaces.datasource.concurrentaccess;

import com.gigaspaces.datasource.DataIterator;

/**
 * Shared iterator shares a single {@link DataIterator} among several instances, each instance gives
 * the impression of a separate data iterator. the sharing is done with the help of a {@link
 * SharedDataIteratorSource} that mediates between the instances and manage the entire cycle
 *
 * @author eitany
 * @since 7.0
 */
@com.gigaspaces.api.InternalApi
public class SharedDataIterator<T> implements DataIterator<T> {
    /** */
    private static final long serialVersionUID = 1L;

    private final SharedDataIteratorSource<T> _source;

    private int _localIteration;

    public SharedDataIterator(SharedDataIteratorSource<T> mediator) {
        _source = mediator;
    }

    public void close() {
        _source.closeSharedIterator();
    }

    public boolean hasNext() {
        return _source.waitForNext(_localIteration);
    }

    public T next() {
        return _source.getNext(_localIteration++);
    }

    public void remove() {
        throw new UnsupportedOperationException();
    }
}

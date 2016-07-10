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

package com.gigaspaces.client.iterator;

import com.gigaspaces.client.ReadModifiers;
import com.gigaspaces.internal.client.spaceproxy.ISpaceProxy;

import net.jini.core.transaction.Transaction;

import java.io.Closeable;
import java.util.Iterator;

/**
 * @author Niv Ingberg
 * @since 10.1
 */
@com.gigaspaces.api.InternalApi
public class SpaceIterator<T> implements Iterator<T>, Iterable<T>, Closeable {
    public static int getDefaultBatchSize() {
        return 100;
    }

    private final SpaceEntryPacketIterator iterator;

    public SpaceIterator(ISpaceProxy spaceProxy, Object query, Transaction txn, int batchSize, ReadModifiers modifiers) {
        this.iterator = new SpaceEntryPacketIterator(spaceProxy, query, txn, batchSize, modifiers.getCode());
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public T next() {
        return (T) iterator.nextEntry();
    }

    @Override
    public void remove() {
        iterator.remove();
    }

    @Override
    public void close() {
        iterator.close();
    }

    @Override
    public Iterator<T> iterator() {
        return this;
    }
}

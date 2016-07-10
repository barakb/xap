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

package com.j_spaces.core.client.version;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;

/**
 * Key in the EntryInfo table.
 *
 * @param <T> the type of the Object that need be mapped to info.
 * @author Guy Korland
 * @version 1.0
 * @since 5.0EAG Build#1400-010
 */
@com.gigaspaces.api.InternalApi
public class EntryInfoKey<T> extends WeakReference<T> {

    // holds the referent hashCode for performance
    protected int _hashCode;

    /**
     * Creates a new key for the {@link VersionTable}.
     *
     * @param referent reference for the Object that should be mapped
     * @param queue    Queue for the cleaner
     */
    public EntryInfoKey(T referent, ReferenceQueue<Object> queue) {
        super(referent, queue);
        init(referent);
    }

    /**
     * Creates a new key for the {@link VersionTable}, used by the find
     */
    public EntryInfoKey(T referent) {
        /**
         * Fix BugID CORE-550: super(T referent) is called and not super(T referent, null)
         * To avoid NPE in 1.4 on IBM JVMs
         */
        super(referent);
        init(referent);
    }

    /**
     * Init the hashcode according to the identity HashCode
     */
    private void init(T referent) {
        _hashCode = System.identityHashCode(referent);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object infoKey) {
        // same Object or the same key value
        // might happen that the local referent is null, but then the two are not equals
        Object value = get();
        return this == infoKey || (value != null && ((EntryInfoKey<T>) infoKey).get() == value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return _hashCode;
    }
}
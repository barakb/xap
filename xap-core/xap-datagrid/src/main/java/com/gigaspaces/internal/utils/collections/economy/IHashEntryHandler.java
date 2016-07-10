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


package com.gigaspaces.internal.utils.collections.economy;


/**
 * TODO	add Javadoc
 *
 * @author Yechiel Fefer
 * @version 1.0
 * @since 5.0
 */
/*
 * handler of hash entries
 */
public interface IHashEntryHandler<K, V> {

    public int hash(IHashEntry<K, V> e);

    public K key(IHashEntry<K, V> e);

    public V value(IHashEntry<K, V> e);

    public IHashEntry<K, V> next(IHashEntry<K, V> e);

    public IHashEntry<K, V> createEntry(K key, V value, IHashEntry<K, V> next, int hash);

    public IHashEntry<K, V> createEntry(K key, V value, IHashEntry<K, V> next, int hash, boolean unstableKey);

    public IHashEntry<K, V> cloneEntry(IHashEntry<K, V> e, IHashEntry<K, V> newNext);

    public IHashEntry<K, V> cloneEntry(IHashEntry<K, V> e, IHashEntry<K, V> newNext, boolean unstableKey);

}

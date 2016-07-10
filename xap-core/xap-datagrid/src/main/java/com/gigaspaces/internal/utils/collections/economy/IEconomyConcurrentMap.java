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

import java.util.concurrent.ConcurrentMap;

/**
 * TODO	add Javadoc
 *
 * @author Yechiel Fefer
 * @version 1.0
 * @since 7.1
 */
/*
 * additional methods for economy map
 */
public interface IEconomyConcurrentMap<K, V>
        extends ConcurrentMap<K, V> {
    //key may be replaced in host (value) object w/o calling the map
    //, prevent inconsistent situations
    void setKeyUnstable(K key);

    V putIfAbsent(K key, V value, boolean unstableKey);


}

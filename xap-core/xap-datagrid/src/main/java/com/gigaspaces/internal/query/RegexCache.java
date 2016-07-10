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


package com.gigaspaces.internal.query;

import com.gigaspaces.internal.server.space.SpaceConfigReader;

import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import static com.j_spaces.core.Constants.Engine.ENGINE_REGULAR_EXPRESSIONS_CACHE_SIZE_DEFAULT;
import static com.j_spaces.core.Constants.Engine.ENGINE_REGULAR_EXPRESSIONS_CACHE_SIZE_PROP;

/**
 * Holds a cache of already compiled regular expressions.
 *
 * @author Guy Korland
 * @since 6.1
 */
@com.gigaspaces.api.InternalApi
public class RegexCache {
    /**
     * compiled regular expressions cache.
     */
    final private int _cacheSize; //max # of reg expressions in cache
    final private ConcurrentHashMap<String, Pattern> _regexCache = new ConcurrentHashMap<String, Pattern>();  //key=string value =Pattern
    final private LinkedList<String> _regexItems = new LinkedList<String>();  // all keys in insert order

    public RegexCache(SpaceConfigReader configReader) {
        int size = configReader.getIntSpaceProperty(
                ENGINE_REGULAR_EXPRESSIONS_CACHE_SIZE_PROP,
                ENGINE_REGULAR_EXPRESSIONS_CACHE_SIZE_DEFAULT);

        if (size < 0)
            throw new IllegalArgumentException("Invalid regular-expressions cache size value specified: " + size);
        _cacheSize = size;
    }

    public RegexCache() {
        _cacheSize = Integer.parseInt(ENGINE_REGULAR_EXPRESSIONS_CACHE_SIZE_DEFAULT);
    }

    /**
     * Gets a compiled regular expression from cache, or save a new one. If the cache size is
     * increased behind the _cacheSize, the first one is cleaned in a FIFO way.
     */
    public Pattern getPattern(String string_exp) throws PatternSyntaxException {
        Pattern p = _regexCache.get(string_exp);
        if (p == null) {
            synchronized (_regexItems) {
                p = _regexCache.get(string_exp);
                if (p == null) {//new regex add to cache
                    Pattern new_p = Pattern.compile(string_exp);
                    if (_regexItems.size() > _cacheSize) {
                        String del = _regexItems.removeFirst();
                        _regexCache.remove(del);
                    }
                    _regexCache.put(string_exp, new_p);
                    _regexItems.addLast(string_exp);
                    p = new_p;
                }
            }//synchronized(m_RegexCache)
        }//if (p == null)
        return p;
    }
}

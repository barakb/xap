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

package com.gigaspaces.internal.transport;


import net.jini.space.InternalSpaceException;

/**
 * Info regarding a projection for a path
 *
 * @author yechiel
 * @since 9.7
 */

@com.gigaspaces.api.InternalApi
public class PathProjectionInfo {

    private final String _fullPath;
    private final String[] _tokens;
    private final boolean[] _collectionsIndicators;
    private final static String COLLECTION_STRING = "[*]";


    PathProjectionInfo(String fullPath) {
        _fullPath = fullPath;
        _tokens = fullPath.split("\\.");
        if (_tokens == null || _tokens.length == 0)
            throw new InternalSpaceException("Error splitting property path [" + fullPath + "].");
        _collectionsIndicators = new boolean[_tokens.length];
        for (int i = 0; i < _tokens.length; i++) {
            String token = removeCollectionInfoIfExist(_tokens[i]);
            if (_tokens[i] != token) {
                _tokens[i] = token;
                _collectionsIndicators[i] = true;
            }
        }
    }

    String getFullPath() {
        return _fullPath;
    }

    String[] getTokens() {
        return _tokens;
    }

    int getDepth() {
        return _tokens.length;
    }

    boolean[] getCollectionIndicators() {
        return _collectionsIndicators;
    }

    static String removeCollectionInfoIfExist(String token) {
        while (token.endsWith(COLLECTION_STRING)) {
            token = token.substring(0, token.length() - COLLECTION_STRING.length());
            token.trim();
        }
        return token;
    }

}

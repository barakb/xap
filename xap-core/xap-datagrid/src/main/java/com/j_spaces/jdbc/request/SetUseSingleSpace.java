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

package com.j_spaces.jdbc.request;

import com.j_spaces.jdbc.QueryHandler;
import com.j_spaces.jdbc.QuerySession;
import com.j_spaces.jdbc.RequestPacket;
import com.j_spaces.jdbc.ResponsePacket;

/**
 * @author anna
 * @since 6.1
 */
@com.gigaspaces.api.InternalApi
public class SetUseSingleSpace extends RequestPacket {
    private static final long serialVersionUID = 5574394936473046090L;

    private boolean _useSingleSpace;

    /**
     *
     */
    public SetUseSingleSpace() {
        super();

    }

    /**
     * @param useSingleSpace
     */
    public SetUseSingleSpace(boolean useSingleSpace) {
        _useSingleSpace = useSingleSpace;
    }

    public boolean isUseSingleSpace() {
        return _useSingleSpace;
    }

    public void setUseSingleSpace(boolean autoCommit) {
        _useSingleSpace = autoCommit;
    }

    @Override
    public ResponsePacket accept(QueryHandler handler, QuerySession session) {
        return handler.visit(this, session);
    }

    @Override
    public String toString() {
        return "set single space " + _useSingleSpace;
    }
}

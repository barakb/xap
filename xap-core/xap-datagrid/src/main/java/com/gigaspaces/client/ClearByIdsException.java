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

package com.gigaspaces.client;

import com.gigaspaces.internal.server.space.ReadByIdsContext;

import java.util.Map;

/**
 * Created by boris on 12/30/2014.
 *
 * Since 10.1.0
 */

@com.gigaspaces.api.InternalApi
public class ClearByIdsException extends RuntimeException {

    private static final long serialVersionUID = 1L;
    private ReadByIdsContext _context;


    public ClearByIdsException(ReadByIdsContext context) {
        this._context = context;
    }

    /**
     * Summarize how many objects got exceptions while getting cleared.
     */
    @Override
    public String getMessage() {
        StringBuilder sb = new StringBuilder();
        sb.append("Number of entries that got exceptions during clear operation: ").append(_context.getFailures().size());
        sb.append('\n').append("Ids which got exceptions during clear operation: ").append('\n');
        for (Map.Entry<Object, Exception> entry : _context.getFailures().entrySet()) {
            sb.append("[")
                    .append("id: ")
                    .append(entry.getKey())
                    .append(" , Exception: ")
                    .append(entry.getValue())
                    .append("]")
                    .append('\n');
        }
        return sb.toString();
    }

    /**
     * @return a map that contains the
     */
    public Map<Object, Exception> getFailures() {
        return _context.getFailures();
    }
}

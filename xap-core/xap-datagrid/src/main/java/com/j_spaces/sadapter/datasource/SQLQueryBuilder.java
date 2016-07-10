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

/**
 *
 */
package com.j_spaces.sadapter.datasource;

import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.server.storage.ITemplateHolder;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.j_spaces.core.client.SQLQuery;

/**
 * Interface for building an {@link SQLQuery} object from space template
 *
 * @author anna
 */
public interface SQLQueryBuilder {
    /**
     * Builds an {@link SQLQuery} from given template
     *
     * @return SQLQuery
     */
    SQLQuery<?> build(ITemplateHolder template, String typeName, ITypeDesc typeDesc);


    /**
     * Builds an {@link SQLQuery} from given entry packet
     *
     * @return SQLQuery
     */
    SQLQuery<?> build(IEntryPacket entryPacket, ITypeDesc typeDesc);
}

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

package com.j_spaces.jdbc.builder;

import com.gigaspaces.internal.client.QueryResultTypeInternal;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.transport.AbstractProjectionTemplate;
import com.gigaspaces.internal.transport.AbstractQueryPacket;
import com.gigaspaces.internal.transport.ProjectionTemplate;
import com.j_spaces.core.client.SQLQuery;

/**
 * A wrapper for the SQLQuery - gives it a facade of ITemplatePacket
 *
 * @author anna
 * @since 7.0
 */
@com.gigaspaces.api.InternalApi
public class SQLQueryTemplatePacket extends AbstractQueryPacket {
    private static final long serialVersionUID = 1L;

    private transient SQLQuery<?> _query;
    private transient AbstractProjectionTemplate _projectionTemplate;

    /**
     * Required for Externalizable
     */
    public SQLQueryTemplatePacket() {
        throw new IllegalStateException("This constructor is required for Externalizable and should not be called directly.");
    }

    public SQLQueryTemplatePacket(SQLQuery<?> sqlQuery, ITypeDesc typeDesc, QueryResultTypeInternal resultType) {
        super(typeDesc, resultType);
        _query = sqlQuery;
        if (sqlQuery.getProjections() != null)
            _projectionTemplate = ProjectionTemplate.create(sqlQuery.getProjections(), typeDesc);
    }

    public SQLQuery<?> getQuery() {
        return _query;
    }

    @Override
    public String getTypeName() {
        return _query.getTypeName();
    }

    @Override
    public AbstractProjectionTemplate getProjectionTemplate() {
        return _projectionTemplate;
    }

    public void setProjectionTemplate(AbstractProjectionTemplate projectionTemplate) {
        _projectionTemplate = projectionTemplate;
    }
}

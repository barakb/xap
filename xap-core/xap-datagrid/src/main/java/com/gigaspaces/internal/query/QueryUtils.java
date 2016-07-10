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

import com.gigaspaces.document.SpaceDocument;
import com.gigaspaces.internal.metadata.SpacePropertyInfo;
import com.gigaspaces.internal.metadata.SpaceTypeInfo;
import com.gigaspaces.internal.metadata.SpaceTypeInfoRepository;
import com.gigaspaces.internal.utils.StringUtils;
import com.gigaspaces.query.ISpaceQuery;
import com.gigaspaces.query.IdQuery;
import com.gigaspaces.query.IdsQuery;
import com.gigaspaces.query.QueryResultType;
import com.j_spaces.core.client.SQLQuery;

import java.util.Map;

/**
 * @author Niv Ingberg
 * @since 9.5.0
 */
@com.gigaspaces.api.InternalApi
public class QueryUtils {
    public static String getQueryDescription(Object query) {
        StringBuilder sb = new StringBuilder();
        getQueryDescription(query, sb);
        return sb.toString();
    }

    public static void getQueryDescription(Object query, StringBuilder sb) {
        if (query == null)
            return;

        if (query instanceof String)
            sb.append(query);
        else if (query instanceof ISpaceQuery) {
            if (query instanceof SQLQuery)
                getSqlQueryDescription((SQLQuery<?>) query, sb);
            else if (query instanceof IdQuery)
                getIdQueryDescription((IdQuery<?>) query, sb);
            else if (query instanceof IdsQuery)
                getIdsQueryDescription((IdsQuery<?>) query, sb);
            else
                sb.append("Unrecognized space query class: " + query.getClass());
        } else if (query instanceof SpaceDocument)
            getDocumentTemplateDescription((SpaceDocument) query, sb);
        else
            getObjectTemplateDescription(query, sb);
    }

    private static void getDocumentTemplateDescription(SpaceDocument template, StringBuilder sb) {
        sb.append("Document Template").append(StringUtils.NEW_LINE);
        appendTypeName(sb, template.getTypeName());
        for (Map.Entry<String, Object> property : template.getProperties().entrySet()) {
            Object propertyValue = property.getValue();
            if (propertyValue != null)
                appendValueWithType(sb, "\t" + property.getKey(), propertyValue);
        }
    }

    private static void getObjectTemplateDescription(Object template, StringBuilder sb) {
        sb.append("Object Template").append(StringUtils.NEW_LINE);
        final SpaceTypeInfo typeInfo = SpaceTypeInfoRepository.getTypeInfo(template.getClass());
        appendTypeName(sb, typeInfo.getName());
        final int numOfProperties = typeInfo.getNumOfSpaceProperties();
        for (int i = 0; i < numOfProperties; i++) {
            SpacePropertyInfo property = typeInfo.getProperty(i);
            Object propertyValue = property.getValue(template);
            if (propertyValue != null)
                appendValueWithType(sb, "\t" + property.getName(), propertyValue);
        }
    }

    private static void getSqlQueryDescription(SQLQuery<?> query, StringBuilder sb) {
        sb.append("SQL Query").append(StringUtils.NEW_LINE);
        appendTypeName(sb, query.getTypeName());
        appendValue(sb, "Criteria", query.getQuery());

        final Object[] parameters = query.getParameters();
        appendValue(sb, "Parameters", parameters == null ? 0 : parameters.length);
        if (parameters != null)
            for (int i = 0; i < parameters.length; i++)
                appendValueWithType(sb, "\t#" + (i + 1), parameters[i]);
        appendValueWithTypeIfNotNull(sb, "Template", query.getObject());
        appendRouting(sb, query.getRouting());
        appendQueryResultType(sb, query.getQueryResultType());
        appendProjections(sb, query.getProjections());
    }

    private static void getIdQueryDescription(IdQuery<?> query, StringBuilder sb) {
        sb.append("ID Query").append(StringUtils.NEW_LINE);
        appendTypeName(sb, query.getTypeName());
        appendValueWithType(sb, "Id", query.getId());
        appendRouting(sb, query.getRouting());
        appendVersion(sb, query.getVersion());
        appendQueryResultType(sb, query.getQueryResultType());
        appendProjections(sb, query.getProjections());
    }

    private static void getIdsQueryDescription(IdsQuery<?> query, StringBuilder sb) {
        sb.append("IDs Query").append(StringUtils.NEW_LINE);
        appendTypeName(sb, query.getTypeName());
        if (query.getRoutings() != null)
            appendIdsWithMultipleRoutings(sb, query.getIds(), query.getRoutings());
        else {
            appendValueWithTypeIfNotNull(sb, "Routing", query.getRouting());
            appendIds(sb, query.getIds());
        }

        appendQueryResultType(sb, query.getQueryResultType());
        appendProjections(sb, query.getProjections());
    }

    private static void appendIdsWithMultipleRoutings(StringBuilder sb, Object[] ids, Object[] routings) {
        appendValue(sb, "ID-Routing pairs", ids.length);
        for (int i = 0; i < ids.length; i++) {
            sb.append("\t#" + (i + 1) + " ");
            appendValueWithType(sb, "ID", ids[i], false);
            appendValueWithType(sb, ", routing", routings[i]);
        }
    }

    private static void appendIds(StringBuilder sb, Object[] ids) {
        appendValue(sb, "IDs", ids.length);
        for (int i = 0; i < ids.length; i++)
            appendValueWithType(sb, "\t#" + (i + 1), ids[i]);
    }

    private static void appendTypeName(StringBuilder sb, String typeName) {
        appendValue(sb, "Entry type", typeName);
    }

    private static void appendRouting(StringBuilder sb, Object routing) {
        if (routing != null)
            appendValueWithType(sb, "Routing", routing);
    }

    private static void appendQueryResultType(StringBuilder sb, QueryResultType queryResultType) {
        if (queryResultType != QueryResultType.DEFAULT && queryResultType != QueryResultType.NOT_SET)
            appendValue(sb, "Query result type", queryResultType);
    }

    private static void appendVersion(StringBuilder sb, int version) {
        if (version != 0)
            appendValue(sb, "Version", version);
    }

    private static void appendProjections(StringBuilder sb, String[] projections) {
        if (projections == null || projections.length == 0)
            return;
        sb.append("Projections: ");
        sb.append(projections[0]);
        for (int i = 1; i < projections.length; i++)
            sb.append(", ").append(projections[i]);
        sb.append(StringUtils.NEW_LINE);
    }

    private static void appendValue(StringBuilder sb, String name, Object value) {
        append(sb, name, value, false, true);
    }

    private static void appendValueWithType(StringBuilder sb, String name, Object value) {
        append(sb, name, value, true, true);
    }

    private static void appendValueWithType(StringBuilder sb, String name, Object value, boolean appendNewLine) {
        append(sb, name, value, true, appendNewLine);
    }

    private static void appendValueWithTypeIfNotNull(StringBuilder sb, String name, Object value) {
        if (value != null)
            append(sb, name, value, true, true);
    }

    private static void append(StringBuilder sb, String name, Object value, boolean includeType, boolean appendNewLine) {
        sb.append(name);
        sb.append(": ");
        if (value == null)
            sb.append("NULL");
        else {
            sb.append(value);
            if (includeType)
                appendType(sb, value.getClass());
        }
        if (appendNewLine)
            sb.append(StringUtils.NEW_LINE);
    }

    private static void appendType(StringBuilder sb, Class<?> type) {
        sb.append(" [").append(type.getName()).append("]");
    }

    private static Class<?> getCommonTypeIfExists(Object[] values) {
        if (values == null || values.length == 0 || values[0] == null)
            return null;
        Class<?> commonType = values[0].getClass();
        for (int i = 1; i < values.length; i++)
            if (values[i] == null || !values[i].getClass().equals(commonType))
                return null;

        return commonType;
    }
}

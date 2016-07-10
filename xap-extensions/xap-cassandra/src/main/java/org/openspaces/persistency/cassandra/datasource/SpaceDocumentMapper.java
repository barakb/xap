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

package org.openspaces.persistency.cassandra.datasource;

import com.gigaspaces.document.SpaceDocument;

import org.openspaces.persistency.cassandra.meta.ColumnFamilyMetadata;
import org.openspaces.persistency.cassandra.meta.ColumnMetadata;
import org.openspaces.persistency.cassandra.meta.DynamicColumnMetadata;
import org.openspaces.persistency.cassandra.meta.data.ColumnData;
import org.openspaces.persistency.cassandra.meta.data.ColumnFamilyRow;
import org.openspaces.persistency.cassandra.meta.data.ColumnFamilyRow.ColumnFamilyRowType;
import org.openspaces.persistency.cassandra.meta.mapping.SpaceDocumentColumnFamilyMapper;

import me.prettyprint.cassandra.service.template.ColumnFamilyResult;
import me.prettyprint.cassandra.service.template.ColumnFamilyRowMapper;

/**
 * A {@link ColumnFamilyRowMapper} implementation. Delegating the actual mapping job to a {@link
 * org.openspaces.persistency.cassandra.meta.mapping.SpaceDocumentColumnFamilyMapper} instance.
 *
 * @author Dan Kilman
 * @since 9.1.1
 */
public class SpaceDocumentMapper
        implements ColumnFamilyRowMapper<Object, String, SpaceDocument> {

    private final SpaceDocumentColumnFamilyMapper mapper;
    private final ColumnFamilyMetadata metadata;

    public SpaceDocumentMapper(
            ColumnFamilyMetadata metadata,
            SpaceDocumentColumnFamilyMapper mapper) {
        this.mapper = mapper;
        this.metadata = metadata;
    }

    @Override
    public SpaceDocument mapRow(ColumnFamilyResult<Object, String> rs) {
        ColumnFamilyRow row = new ColumnFamilyRow(metadata,
                rs.getKey(),
                ColumnFamilyRowType.Read);

        for (String columnName : rs.getColumnNames()) {
            ColumnMetadata columnMetadata = metadata.getColumns().get(columnName);
            if (columnMetadata == null) {
                columnMetadata = new DynamicColumnMetadata(columnName,
                        mapper.getTypeNodeIntrospector()
                                .getDynamicPropertyValueSerializer());
            }
            Object columnValue = columnMetadata.getSerializer().fromByteBuffer(rs.getColumn(columnName).getValueBytes());
            row.addColumnData(new ColumnData(columnValue, columnMetadata));
        }

        return mapper.toDocument(row);
    }

}

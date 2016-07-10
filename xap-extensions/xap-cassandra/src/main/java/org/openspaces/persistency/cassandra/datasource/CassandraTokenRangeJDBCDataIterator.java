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

import com.gigaspaces.datasource.DataIterator;
import com.gigaspaces.document.SpaceDocument;
import com.gigaspaces.internal.utils.StringUtils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openspaces.persistency.cassandra.CassandraConsistencyLevel;
import org.openspaces.persistency.cassandra.error.SpaceCassandraQueryExecutionException;
import org.openspaces.persistency.cassandra.meta.ColumnFamilyMetadata;
import org.openspaces.persistency.cassandra.meta.ColumnMetadata;
import org.openspaces.persistency.cassandra.meta.DynamicColumnMetadata;
import org.openspaces.persistency.cassandra.meta.data.ColumnData;
import org.openspaces.persistency.cassandra.meta.data.ColumnFamilyRow;
import org.openspaces.persistency.cassandra.meta.data.ColumnFamilyRow.ColumnFamilyRowType;
import org.openspaces.persistency.cassandra.meta.mapping.SpaceDocumentColumnFamilyMapper;
import org.openspaces.persistency.cassandra.meta.types.dynamic.DynamicPropertySerializer;
import org.openspaces.persistency.cassandra.pool.ConnectionResource;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.sql.SQLTransientConnectionException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import me.prettyprint.hector.api.Serializer;

/**
 * Note: the underlying cassandra jdbc implementation brings the entire batch when calling execute,
 * so SQLException exceptions will actually only be thrown during prepare statement and execute
 * statment Moreover, the iteration over the results is currently only logical because the entire
 * result set already lies in memory after execute statement returns.
 *
 * @author Dan Kilman
 * @since 9.1.1
 */
public class CassandraTokenRangeJDBCDataIterator implements DataIterator<Object> {

    private static final Log logger = LogFactory.getLog(CassandraTokenRangeJDBCDataIterator.class);

    private static final String IDENTIFIER = "((?:[a-zA-Z_$][a-zA-Z\\d_$]*\\.)*[a-zA-Z_$][a-zA-Z\\d_$]*)";
    private static final String OPERATOR = "(?:=|<|>|<=|>=)";
    private static final String COLUMN_NAME_REGEX = "(?:" + IDENTIFIER + "\\s*" + OPERATOR + "\\s*?)";
    private static final Pattern COLUMN_NAME_PATTERN = Pattern.compile(COLUMN_NAME_REGEX);

    private final SpaceDocumentColumnFamilyMapper mapper;
    private final ColumnFamilyMetadata columnFamilyMetadata;
    private final int limit;
    private final CassandraConsistencyLevel readConsistencyLevel;
    private final ConnectionResource connectionResource;
    private final PreparedStatement preparedStatement;
    private final ResultSet resultSet;

    private Object currentLastToken;
    private SpaceDocument currentResultInResultSet;
    private int currentTotalCount = 0;


    public CassandraTokenRangeJDBCDataIterator(
            SpaceDocumentColumnFamilyMapper mapper,
            ColumnFamilyMetadata columnFamilyMetadata,
            ConnectionResource connectionResource,
            CQLQueryContext queryContext,
            Object lastToken,
            int limit,
            CassandraConsistencyLevel readConsistencyLevel) {

        if (logger.isTraceEnabled()) {
            logger.trace("Creating range data iterator for query: " + queryContext + " for type: " + columnFamilyMetadata.getTypeName() +
                    ", limit=" + limit + ", starting from token(" + (lastToken != null ? lastToken : "FIRST_IN_RING") + ")");
        }

        this.mapper = mapper;
        this.columnFamilyMetadata = columnFamilyMetadata;
        this.connectionResource = connectionResource;
        this.limit = limit;
        this.readConsistencyLevel = readConsistencyLevel;

        Connection connection = connectionResource.getConnection();

        PreparedStatementData statementData = generateSqlQuery(queryContext);
        prepareRangeAndLimitStatement(statementData, lastToken, limit);

        try {
            // this is where cassandra validates this query
            preparedStatement = connection.prepareStatement(statementData.query);
            setPreparedStatementParameters(statementData);
        } catch (SQLSyntaxErrorException e) {
            // no need to replace underlying connection in pool
            throw new SpaceCassandraQueryExecutionException("Failed preparing statement " +
                    statementData.query, e);
        } catch (SQLException e) {
            // need to restart connection
            connectionResource.closeCurrentConnection();
            throw new SpaceCassandraQueryExecutionException("Failed preparing statement " +
                    statementData.query, e);
        }

        try {
            resultSet = preparedStatement.executeQuery();
        } catch (SQLSyntaxErrorException e) {
            // no need to replace underlying connection in pool
            throw new SpaceCassandraQueryExecutionException("Failed executing statement " +
                    statementData.query, e);
        } catch (SQLTransientConnectionException e) {
            // no need to replace underlying connection in pool
            throw new SpaceCassandraQueryExecutionException("Failed executing statement " +
                    statementData.query, e);
        } catch (SQLException e) {
            // need to restart connection
            connectionResource.closeCurrentConnection();
            throw new SpaceCassandraQueryExecutionException("Failed executing statement " +
                    statementData.query, e);
        }

        // we have to read the first entry for the first token in this batch
        // so we call hasNext() internally which fetches the first valid document and cache the first result
        hasNext();
    }

    @Override
    public boolean hasNext() {
        if (currentResultInResultSet != null) {
            return true;
        }

        try {
            currentResultInResultSet = getNextValidDocument();
            return currentResultInResultSet != null;
        } catch (SQLException e) {
            throw new SpaceCassandraQueryExecutionException("Failed checking for any remaining entries ", e);
        }
    }

    private SpaceDocument getNextDocument() throws SQLException {
        List<ColumnData> columns = new LinkedList<ColumnData>();
        Object keyValue = null;

        ResultSetMetaData metaData = resultSet.getMetaData();

        int columnCount = metaData.getColumnCount();
        for (int i = 1; i <= columnCount; i++) {
            String columnName = metaData.getColumnName(i);
            Object columnValue = resultSet.getObject(i);
            if (columnFamilyMetadata.getKeyName().equals(columnName)) {
                keyValue = columnValue;
                if (keyValue instanceof ByteBuffer) {
                    keyValue = columnFamilyMetadata.getKeySerializer().fromByteBuffer((ByteBuffer) keyValue);
                }
            } else {
                ColumnMetadata columnMetadata = columnFamilyMetadata.getColumns().get(columnName);
                if (columnMetadata == null) {
                    columnMetadata = new DynamicColumnMetadata(columnName,
                            mapper.getTypeNodeIntrospector().getDynamicPropertyValueSerializer());
                }

                if (columnValue instanceof ByteBuffer) {
                    columnValue = columnMetadata.getSerializer().fromByteBuffer((ByteBuffer) columnValue);
                }

                columns.add(new ColumnData(columnValue, columnMetadata));
            }
        }

        ColumnFamilyRow row = new ColumnFamilyRow(columnFamilyMetadata,
                keyValue,
                ColumnFamilyRowType.Read);
        for (ColumnData columnData : columns) {
            row.addColumnData(columnData);
        }

        return mapper.toDocument(row);
    }

    private SpaceDocument getNextValidDocument() throws SQLException {
        while (resultSet.next()) {
            currentTotalCount++;
            SpaceDocument document = getNextDocument();
            Object currentTokenValue = document.getProperty(columnFamilyMetadata.getKeyName());
            currentLastToken = currentTokenValue;
            if (document.getProperties().size() > 1) {
                return document;
            }
        }
        return null;
    }

    @Override
    public SpaceDocument next() {

        SpaceDocument result;
        if (currentResultInResultSet != null) {
            result = currentResultInResultSet;
            currentResultInResultSet = null;
        } else {
            try {
                result = getNextValidDocument();
            } catch (SQLException e) {
                throw new SpaceCassandraQueryExecutionException("Failed retrieving next entry", e);
            }
        }

        return result;
    }

    private PreparedStatementData generateSqlQuery(CQLQueryContext queryContext) {

        PreparedStatementData result = new PreparedStatementData();

        StringBuilder sqlQuery = new StringBuilder();
        sqlQuery.append("SELECT * FROM ").append(quote(columnFamilyMetadata.getColumnFamilyName()));

        // set consistency level 
        sqlQuery.append(" USING CONSISTENCY ").append(readConsistencyLevel.name());

        // query will be null when we got here through initial data load
        if (queryContext == null) {
            result.query = sqlQuery.toString();
            return result;
        }

        if (queryContext.hasProperties() && !queryContext.getProperties().isEmpty()) {
            int propertyIndex = 1;
            for (Map.Entry<String, Object> entry : queryContext.getProperties().entrySet()) {
                if (entry.getValue() == null) {
                    continue;
                }

                sqlQuery
                        .append(" ")
                        .append(propertyIndex == 1 ? "WHERE" : "AND").append(" ")
                        .append(quote(entry.getKey())).append(" = ? ");

                result.propertyValues.add(entry.getValue());

                Serializer<Object> serailizer = getSerializer(entry.getKey());
                result.serializers.add(serailizer);

                propertyIndex++;
            }
        } else if (StringUtils.hasText(queryContext.getSqlQuery())) {
            String selectSqlQuery = queryContext.getSqlQuery();
            Matcher columnNamesMatcher = COLUMN_NAME_PATTERN.matcher(selectSqlQuery);
            while (columnNamesMatcher.find()) {
                String columnName = columnNamesMatcher.group(1);

                Serializer<Object> serailizer = getSerializer(columnName);
                result.serializers.add(serailizer);

                selectSqlQuery = selectSqlQuery.replace(columnName, quote(columnName));
            }

            sqlQuery.append(" WHERE ").append(selectSqlQuery);
            for (int i = 0; i < queryContext.getParameters().length; i++) {
                result.propertyValues.add(queryContext.getParameters()[i]);
            }
        }

        result.query = sqlQuery.toString();
        return result;
    }

    @SuppressWarnings("unchecked")
    private Serializer<Object> getSerializer(String columnName) {
        Serializer<Object> serailizer;
        if (columnFamilyMetadata.getKeyName().equals(columnName)) {
            serailizer = (Serializer<Object>) columnFamilyMetadata.getKeySerializer();
        } else if (columnFamilyMetadata.getColumns().containsKey(columnName)) {
            serailizer = columnFamilyMetadata.getColumns().get(columnName).getSerializer();
        } else {
            serailizer = DynamicPropertySerializer.get();
        }
        return serailizer;
    }

    private void prepareRangeAndLimitStatement(PreparedStatementData statementData, Object lastToken, long limit) {
        StringBuilder newQuery = new StringBuilder(statementData.query);

        if (lastToken != null) {
            if (statementData.query.toUpperCase().contains(" WHERE ")) {
                newQuery.append(" AND ");
            } else {
                newQuery.append(" WHERE ");
            }

            newQuery.append(quote(columnFamilyMetadata.getKeyName())).append(" > ? ");

            statementData.propertyValues.add(lastToken);
            statementData.serializers.add(getSerializer(columnFamilyMetadata.getKeyName()));
        }

        // set limit
        newQuery.append(" LIMIT ").append(limit);

        statementData.query = newQuery.toString();
    }

    private void setPreparedStatementParameters(
            PreparedStatementData statementData) throws SQLException {
        for (int i = 0; i < statementData.propertyValues.size(); i++) {
            int index = i + 1;
            Object propertyValue = statementData.propertyValues.get(i);
            Serializer<Object> serializer = statementData.serializers.get(i);
            byte[] serializedPropertyValue = serializer.toBytes(propertyValue);
            preparedStatement.setBytes(index, serializedPropertyValue);
        }
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("Remove is not supported");
    }

    public void closeSelfResources() {
        try {
            if (!resultSet.isClosed()) {
                resultSet.close();
            }
            if (!preparedStatement.isClosed()) {
                preparedStatement.close();
            }
        } catch (SQLException e) {
            logger.debug("Failed closing result set or prepared statement", e);
        }
    }

    @Override
    public void close() {
        closeSelfResources();
        connectionResource.release();
    }

    public Object getLastToken() {
        return currentLastToken;
    }

    public int getLimit() {
        return limit;
    }

    public int getCurrentTotalCount() {
        return currentTotalCount;
    }

    private static String quote(String s) {
        return s == null ? null : "'" + s + "'";
    }

    private static class PreparedStatementData {
        // order matters
        // arrays list for random access
        List<Serializer<Object>> serializers = new ArrayList<Serializer<Object>>();
        List<Object> propertyValues = new ArrayList<Object>();
        String query;
    }
}

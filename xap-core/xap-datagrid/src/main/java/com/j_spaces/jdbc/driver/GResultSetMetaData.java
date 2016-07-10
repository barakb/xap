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

package com.j_spaces.jdbc.driver;

import com.j_spaces.jdbc.ResultEntry;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;

/**
 * This is the ResultSetMetaData implementation
 *
 * @author Michael Mitrani, 2Train4, 2004
 */
@com.gigaspaces.api.InternalApi
public class GResultSetMetaData implements ResultSetMetaData {

    private static final int DISPLAY_SIZE = 64;
    private final ResultEntry results;

    public GResultSetMetaData(ResultEntry results) {
        this.results = results;
    }

    public int getColumnCount() throws SQLException {
        return results.getFieldNames().length;
    }

    /* (non-Javadoc)
     * @see java.sql.ResultSetMetaData#getColumnDisplaySize(int)
     */
    public int getColumnDisplaySize(int column) throws SQLException {
        return DISPLAY_SIZE;
    }

    /* (non-Javadoc)
     * @see java.sql.ResultSetMetaData#getColumnType(int)
     */
    public int getColumnType(int column) throws SQLException {
        //TODO
        int type = 0;
        String clName = this.getColumnClassName(column);

        if (clName.equals(String.class.getName()))
            type = Types.VARCHAR;
        else if (clName.equals(Integer.class.getName()))
            type = Types.INTEGER;
        else if (clName.equals(Double.class.getName()))
            type = Types.DOUBLE;
        else if (clName.equals(Float.class.getName()))
            type = Types.FLOAT;
        else if (clName.equals(Object.class.getName()))
            type = Types.JAVA_OBJECT;
        else if (clName.equals(Timestamp.class.getName()))
            type = Types.TIMESTAMP;
        else if (clName.equals(Time.class.getName()))
            type = Types.TIME;
        else
            type = Types.OTHER;

        return type;
    }

    /* (non-Javadoc)
     * @see java.sql.ResultSetMetaData#getPrecision(int)
     */
    public int getPrecision(int column) throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    /* (non-Javadoc)
     * @see java.sql.ResultSetMetaData#getScale(int)
     */
    public int getScale(int column) throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    /* (non-Javadoc)
     * @see java.sql.ResultSetMetaData#isNullable(int)
     */
    public int isNullable(int column) throws SQLException {
        return columnNullable;
    }

    /* (non-Javadoc)
     * @see java.sql.ResultSetMetaData#isAutoIncrement(int)
     */
    public boolean isAutoIncrement(int column) throws SQLException {
        return false;
    }

    /* (non-Javadoc)
     * @see java.sql.ResultSetMetaData#isCaseSensitive(int)
     */
    public boolean isCaseSensitive(int column) throws SQLException {
        return true;
    }

    /* (non-Javadoc)
     * @see java.sql.ResultSetMetaData#isCurrency(int)
     */
    public boolean isCurrency(int column) throws SQLException {
        return false;
    }

    /* (non-Javadoc)
     * @see java.sql.ResultSetMetaData#isDefinitelyWritable(int)
     */
    public boolean isDefinitelyWritable(int column) throws SQLException {
        return false;
    }

    /* (non-Javadoc)
     * @see java.sql.ResultSetMetaData#isReadOnly(int)
     */
    public boolean isReadOnly(int column) throws SQLException {
        return false;
    }

    /* (non-Javadoc)
     * @see java.sql.ResultSetMetaData#isSearchable(int)
     */
    public boolean isSearchable(int column) throws SQLException {
        return true;
    }

    /* (non-Javadoc)
     * @see java.sql.ResultSetMetaData#isSigned(int)
     */
    public boolean isSigned(int column) throws SQLException {
        Object[] colObj = results.getFieldValues(1);
        if (colObj == null)
            return false;

        return (colObj[column - 1] instanceof Number);
    }

    /* (non-Javadoc)
     * @see java.sql.ResultSetMetaData#isWritable(int)
     */
    public boolean isWritable(int column) throws SQLException {
        return true;
    }

    /* (non-Javadoc)
     * @see java.sql.ResultSetMetaData#getCatalogName(int)
     */
    public String getCatalogName(int column) throws SQLException {
        return "";
    }

    /* (non-Javadoc)
     * @see java.sql.ResultSetMetaData#getColumnClassName(int)
     */
    public String getColumnClassName(int column)
            throws SQLException {
        if (results != null && results.getFieldValues(1) != null
                && results.getFieldValues(1)[column - 1] != null) {
            return results.getFieldValues(1)[column - 1].getClass().getName();
        } else
            return "";
    }

    /* (non-Javadoc)
     * @see java.sql.ResultSetMetaData#getColumnLabel(int)
     */
    public String getColumnLabel(int column) throws SQLException {
        return results.getColumnLabels()[column - 1];
    }

    /* (non-Javadoc)
     * @see java.sql.ResultSetMetaData#getColumnName(int)
     */
    public String getColumnName(int column) throws SQLException {
        return results.getFieldNames()[column - 1];
    }

    /* (non-Javadoc)
     * @see java.sql.ResultSetMetaData#getColumnTypeName(int)
     */
    public String getColumnTypeName(int column) throws SQLException {
        // TODO Auto-generated method stub
        return "";
    }

    /* (non-Javadoc)
     * @see java.sql.ResultSetMetaData#getSchemaName(int)
     */
    public String getSchemaName(int column) throws SQLException {
        return "";
    }

    /* (non-Javadoc)
     * @see java.sql.ResultSetMetaData#getTableName(int)
     */
    public String getTableName(int column) throws SQLException {
        return results.getTableNames()[column - 1];
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new UnsupportedOperationException();
    }

}

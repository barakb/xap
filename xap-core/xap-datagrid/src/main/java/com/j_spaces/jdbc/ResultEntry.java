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

package com.j_spaces.jdbc;

import java.io.Serializable;

/**
 * This is the result from which a ResultSet will be constructed in response to a SELECT query. This
 * class can be used when calling procedure classes to construct the {@link
 * com.j_spaces.jdbc.IProcedure} execute method return value.
 *
 * The result consists of the field/column names, labels, tables and values.
 *
 * @author Michael Mitrani, 2004
 */
@com.gigaspaces.api.InternalApi
public class ResultEntry implements Serializable {

    /**
     *
     */
    private static final long serialVersionUID = 1L;

    private String[] fieldNames;
    private Object[][] fieldValues;

    /**
     * The column aliases are stored in this array. If no alias was set, the label stores the column
     * name.
     */
    private String[] _columnLabels;

    /**
     * The table name for each column is stored in this array.
     */
    private String[] _tableNames;

    public ResultEntry() {

    }

    public ResultEntry(String[] columnNames, String[] columnLabels, String[] tableNames, Object[][] resultValues) {
        fieldNames = columnNames;
        _columnLabels = columnLabels;
        _tableNames = tableNames;
        fieldValues = resultValues;
    }

    /**
     * The ResultEntry Field Names
     *
     * @return Returns ResultEntry Field Names
     */
    public String[] getFieldNames() {
        return fieldNames;
    }

    /**
     * Set the ResultEntry Field Names
     *
     * @param fieldNames ResultEntry field Names
     */
    public void setFieldNames(String[] fieldNames) {
        this.fieldNames = fieldNames;
    }

    /**
     * Get the ResultEntry's column labels
     *
     * @return ResultEntry's column labels array
     */
    public String[] getColumnLabels() {
        return _columnLabels;
    }

    /**
     * Set the ResultEntry's column labels
     *
     * @param columnLabels ResultEntry's column labels array
     */
    public void setColumnLabels(String[] columnLabels) {
        this._columnLabels = columnLabels;
    }

    /**
     * Set the ResultEntry's table names
     *
     * @param tableNames ResultEntry's table names array
     */
    public void setTableNames(String[] tableNames) {
        this._tableNames = tableNames;
    }

    /**
     * Get the ResultEntry's table names
     *
     * @return ResultEntry's table names array
     */
    public String[] getTableNames() {
        return _tableNames;
    }


    /**
     * The ResultEntry Field Values
     *
     * @param row row number to get data from
     * @return the row data in Object array form
     */
    public Object[] getFieldValues(int row) {
        //row start at 1
        if (row > fieldValues.length)
            return null;
        return fieldValues[row - 1];
    }

    /**
     * Set the ResultEntry Field Values
     *
     * @param fieldValues 2 dimensional object array
     */
    public void setFieldValues(Object[][] fieldValues) {
        this.fieldValues = fieldValues;
    }

    /**
     * Get the ResultEntry Field Values
     *
     * @return 2 dimensional object array consist of ResultEntry data
     */
    public Object[][] getFieldValues() {
        return this.fieldValues;
    }

    /**
     * Get the number of rows in the result set
     *
     * @return the number of rows in the result set
     */
    public int getRowNumber() {
        return ((fieldValues != null) ? fieldValues.length : 0);
    }

    /**
     * @return the result set data
     */
    public String toString() {
        if (fieldValues == null)
            return ("No entries found");
        StringBuilder buffer = new StringBuilder();
        buffer.append("Fields:");
        for (int j = 0; j < fieldValues.length; j++) {
            buffer.append("\n[");
            for (int i = 0; i < fieldNames.length; i++) {
                buffer.append(' ');
                buffer.append(fieldNames[i]);
                buffer.append('=');
                buffer.append(fieldValues[j][i]);
                buffer.append(' ');
            }
            buffer.append(']');
        }
        return buffer.toString();
    }

}

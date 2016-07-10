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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;

/**
 * The DatabaseMetaData implementation
 *
 * @author Michael Mitrani, 2Train4
 */
@com.gigaspaces.api.InternalApi
public class GDatabaseMetaData implements DatabaseMetaData {

    private static final int MAJOR_VERSION = 1;
    private static final int MINOR_VERSION = 1;
    private static final String PRODUCT_NAME = "GigaSpaces DB";
    private final GConnection connection;

    public GDatabaseMetaData(GConnection connection) {
        this.connection = connection;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#getDatabaseMajorVersion()
     */
    public int getDatabaseMajorVersion() throws SQLException {
        return MAJOR_VERSION;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#getDatabaseMinorVersion()
     */
    public int getDatabaseMinorVersion() throws SQLException {
        return MINOR_VERSION;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#getDefaultTransactionIsolation()
     */
    public int getDefaultTransactionIsolation() throws SQLException {
        return Connection.TRANSACTION_READ_UNCOMMITTED;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#getDriverMajorVersion()
     */
    public int getDriverMajorVersion() {
        return GDriver.MAJOR_VERSION;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#getDriverMinorVersion()
     */
    public int getDriverMinorVersion() {
        return GDriver.MINOR_VERSION;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#getJDBCMajorVersion()
     */
    public int getJDBCMajorVersion() throws SQLException {
        return 2;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#getJDBCMinorVersion()
     */
    public int getJDBCMinorVersion() throws SQLException {
        return 0;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#getMaxBinaryLiteralLength()
     */
    public int getMaxBinaryLiteralLength() throws SQLException {
        return 0;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#getMaxCatalogNameLength()
     */
    public int getMaxCatalogNameLength() throws SQLException {
        return 0;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#getMaxCharLiteralLength()
     */
    public int getMaxCharLiteralLength() throws SQLException {
        return 0;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#getMaxColumnNameLength()
     */
    public int getMaxColumnNameLength() throws SQLException {
        return 0;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#getMaxColumnsInGroupBy()
     */
    public int getMaxColumnsInGroupBy() throws SQLException {
        return 0;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#getMaxColumnsInIndex()
     */
    public int getMaxColumnsInIndex() throws SQLException {
        return 0;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#getMaxColumnsInOrderBy()
     */
    public int getMaxColumnsInOrderBy() throws SQLException {
        return 0;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#getMaxColumnsInSelect()
     */
    public int getMaxColumnsInSelect() throws SQLException {
        return 0;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#getMaxColumnsInTable()
     */
    public int getMaxColumnsInTable() throws SQLException {
        return 0;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#getMaxConnections()
     */
    public int getMaxConnections() throws SQLException {
        return 0;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#getMaxCursorNameLength()
     */
    public int getMaxCursorNameLength() throws SQLException {
        return 0;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#getMaxIndexLength()
     */
    public int getMaxIndexLength() throws SQLException {
        return 0;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#getMaxProcedureNameLength()
     */
    public int getMaxProcedureNameLength() throws SQLException {
        return 0;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#getMaxRowSize()
     */
    public int getMaxRowSize() throws SQLException {
        return 0;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#getMaxSchemaNameLength()
     */
    public int getMaxSchemaNameLength() throws SQLException {
        return 0;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#getMaxStatementLength()
     */
    public int getMaxStatementLength() throws SQLException {
        return 0;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#getMaxStatements()
     */
    public int getMaxStatements() throws SQLException {
        return 0;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#getMaxTableNameLength()
     */
    public int getMaxTableNameLength() throws SQLException {
        return 0;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#getMaxTablesInSelect()
     */
    public int getMaxTablesInSelect() throws SQLException {
        return 1;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#getMaxUserNameLength()
     */
    public int getMaxUserNameLength() throws SQLException {
        //no limit
        return 0;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#getResultSetHoldability()
     */
    public int getResultSetHoldability() throws SQLException {
        throw new SQLException("Not Supported!");
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#getSQLStateType()
     */
    public int getSQLStateType() throws SQLException {
        throw new SQLException("Not Supported!");
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#allProceduresAreCallable()
     */
    public boolean allProceduresAreCallable() throws SQLException {
        return false;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#allTablesAreSelectable()
     */
    public boolean allTablesAreSelectable() throws SQLException {
        return true;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#dataDefinitionCausesTransactionCommit()
     */
    public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
        return false;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#dataDefinitionIgnoredInTransactions()
     */
    public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
        return false;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#doesMaxRowSizeIncludeBlobs()
     */
    public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
        return false;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#isCatalogAtStart()
     */
    public boolean isCatalogAtStart() throws SQLException {
        return false;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#isReadOnly()
     */
    public boolean isReadOnly() throws SQLException {
        return false;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#locatorsUpdateCopy()
     */
    public boolean locatorsUpdateCopy() throws SQLException {
        return false;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#nullPlusNonNullIsNull()
     */
    public boolean nullPlusNonNullIsNull() throws SQLException {
        return false;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#nullsAreSortedAtEnd()
     */
    public boolean nullsAreSortedAtEnd() throws SQLException {
        return false;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#nullsAreSortedAtStart()
     */
    public boolean nullsAreSortedAtStart() throws SQLException {
        return false;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#nullsAreSortedHigh()
     */
    public boolean nullsAreSortedHigh() throws SQLException {
        return false;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#nullsAreSortedLow()
     */
    public boolean nullsAreSortedLow() throws SQLException {
        return false;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#storesLowerCaseIdentifiers()
     */
    public boolean storesLowerCaseIdentifiers() throws SQLException {
        return false;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#storesLowerCaseQuotedIdentifiers()
     */
    public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#storesMixedCaseIdentifiers()
     */
    public boolean storesMixedCaseIdentifiers() throws SQLException {
        return true;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#storesMixedCaseQuotedIdentifiers()
     */
    public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
        return true;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#storesUpperCaseIdentifiers()
     */
    public boolean storesUpperCaseIdentifiers() throws SQLException {
        return false;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#storesUpperCaseQuotedIdentifiers()
     */
    public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#supportsANSI92EntryLevelSQL()
     */
    public boolean supportsANSI92EntryLevelSQL() throws SQLException {
        return false;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#supportsANSI92FullSQL()
     */
    public boolean supportsANSI92FullSQL() throws SQLException {
        return false;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#supportsANSI92IntermediateSQL()
     */
    public boolean supportsANSI92IntermediateSQL() throws SQLException {
        return false;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#supportsAlterTableWithAddColumn()
     */
    public boolean supportsAlterTableWithAddColumn() throws SQLException {
        return false;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#supportsAlterTableWithDropColumn()
     */
    public boolean supportsAlterTableWithDropColumn() throws SQLException {
        return false;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#supportsBatchUpdates()
     */
    public boolean supportsBatchUpdates() throws SQLException {
        return true;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#supportsCatalogsInDataManipulation()
     */
    public boolean supportsCatalogsInDataManipulation() throws SQLException {
        return false;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#supportsCatalogsInIndexDefinitions()
     */
    public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
        return false;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#supportsCatalogsInPrivilegeDefinitions()
     */
    public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
        return false;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#supportsCatalogsInProcedureCalls()
     */
    public boolean supportsCatalogsInProcedureCalls() throws SQLException {
        return false;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#supportsCatalogsInTableDefinitions()
     */
    public boolean supportsCatalogsInTableDefinitions() throws SQLException {
        return false;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#supportsColumnAliasing()
     */
    public boolean supportsColumnAliasing() throws SQLException {
        return false;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#supportsConvert()
     */
    public boolean supportsConvert() throws SQLException {
        return false;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#supportsCoreSQLGrammar()
     */
    public boolean supportsCoreSQLGrammar() throws SQLException {
        return false;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#supportsCorrelatedSubqueries()
     */
    public boolean supportsCorrelatedSubqueries() throws SQLException {
        return false;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#supportsDataDefinitionAndDataManipulationTransactions()
     */
    public boolean supportsDataDefinitionAndDataManipulationTransactions()
            throws SQLException {
        return true;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#supportsDataManipulationTransactionsOnly()
     */
    public boolean supportsDataManipulationTransactionsOnly()
            throws SQLException {
        return false;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#supportsDifferentTableCorrelationNames()
     */
    public boolean supportsDifferentTableCorrelationNames() throws SQLException {
        return false;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#supportsExpressionsInOrderBy()
     */
    public boolean supportsExpressionsInOrderBy() throws SQLException {
        return false;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#supportsExtendedSQLGrammar()
     */
    public boolean supportsExtendedSQLGrammar() throws SQLException {
        return false;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#supportsFullOuterJoins()
     */
    public boolean supportsFullOuterJoins() throws SQLException {
        return false;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#supportsGetGeneratedKeys()
     */
    public boolean supportsGetGeneratedKeys() throws SQLException {
        return false;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#supportsGroupBy()
     */
    public boolean supportsGroupBy() throws SQLException {
        return false;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#supportsGroupByBeyondSelect()
     */
    public boolean supportsGroupByBeyondSelect() throws SQLException {
        return false;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#supportsGroupByUnrelated()
     */
    public boolean supportsGroupByUnrelated() throws SQLException {
        return false;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#supportsIntegrityEnhancementFacility()
     */
    public boolean supportsIntegrityEnhancementFacility() throws SQLException {
        return false;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#supportsLikeEscapeClause()
     */
    public boolean supportsLikeEscapeClause() throws SQLException {
        return false;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#supportsLimitedOuterJoins()
     */
    public boolean supportsLimitedOuterJoins() throws SQLException {
        return false;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#supportsMinimumSQLGrammar()
     */
    public boolean supportsMinimumSQLGrammar() throws SQLException {
        return false;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#supportsMixedCaseIdentifiers()
     */
    public boolean supportsMixedCaseIdentifiers() throws SQLException {
        return false;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#supportsMixedCaseQuotedIdentifiers()
     */
    public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#supportsMultipleOpenResults()
     */
    public boolean supportsMultipleOpenResults() throws SQLException {
        return false;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#supportsMultipleResultSets()
     */
    public boolean supportsMultipleResultSets() throws SQLException {
        return false;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#supportsMultipleTransactions()
     */
    public boolean supportsMultipleTransactions() throws SQLException {
        return false;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#supportsNamedParameters()
     */
    public boolean supportsNamedParameters() throws SQLException {
        return false;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#supportsNonNullableColumns()
     */
    public boolean supportsNonNullableColumns() throws SQLException {
        return false;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#supportsOpenCursorsAcrossCommit()
     */
    public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
        return false;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#supportsOpenCursorsAcrossRollback()
     */
    public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
        return false;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#supportsOpenStatementsAcrossCommit()
     */
    public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
        return false;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#supportsOpenStatementsAcrossRollback()
     */
    public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
        return false;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#supportsOrderByUnrelated()
     */
    public boolean supportsOrderByUnrelated() throws SQLException {
        return false;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#supportsOuterJoins()
     */
    public boolean supportsOuterJoins() throws SQLException {
        return false;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#supportsPositionedDelete()
     */
    public boolean supportsPositionedDelete() throws SQLException {
        return false;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#supportsPositionedUpdate()
     */
    public boolean supportsPositionedUpdate() throws SQLException {
        return false;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#supportsSavepoints()
     */
    public boolean supportsSavepoints() throws SQLException {
        return false;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#supportsSchemasInDataManipulation()
     */
    public boolean supportsSchemasInDataManipulation() throws SQLException {
        return false;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#supportsSchemasInIndexDefinitions()
     */
    public boolean supportsSchemasInIndexDefinitions() throws SQLException {
        return false;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#supportsSchemasInPrivilegeDefinitions()
     */
    public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
        return false;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#supportsSchemasInProcedureCalls()
     */
    public boolean supportsSchemasInProcedureCalls() throws SQLException {
        return false;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#supportsSchemasInTableDefinitions()
     */
    public boolean supportsSchemasInTableDefinitions() throws SQLException {
        return false;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#supportsSelectForUpdate()
     */
    public boolean supportsSelectForUpdate() throws SQLException {
        return false;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#supportsStatementPooling()
     */
    public boolean supportsStatementPooling() throws SQLException {
        return false;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#supportsStoredProcedures()
     */
    public boolean supportsStoredProcedures() throws SQLException {
        return false;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#supportsSubqueriesInComparisons()
     */
    public boolean supportsSubqueriesInComparisons() throws SQLException {
        return false;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#supportsSubqueriesInExists()
     */
    public boolean supportsSubqueriesInExists() throws SQLException {
        return false;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#supportsSubqueriesInIns()
     */
    public boolean supportsSubqueriesInIns() throws SQLException {
        return false;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#supportsSubqueriesInQuantifieds()
     */
    public boolean supportsSubqueriesInQuantifieds() throws SQLException {
        return false;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#supportsTableCorrelationNames()
     */
    public boolean supportsTableCorrelationNames() throws SQLException {
        return false;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#supportsTransactions()
     */
    public boolean supportsTransactions() throws SQLException {
        return true;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#supportsUnion()
     */
    public boolean supportsUnion() throws SQLException {
        return false;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#supportsUnionAll()
     */
    public boolean supportsUnionAll() throws SQLException {
        return false;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#usesLocalFilePerTable()
     */
    public boolean usesLocalFilePerTable() throws SQLException {
        return false;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#usesLocalFiles()
     */
    public boolean usesLocalFiles() throws SQLException {
        return false;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#deletesAreDetected(int)
     */
    public boolean deletesAreDetected(int type) throws SQLException {
        return false;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#insertsAreDetected(int)
     */
    public boolean insertsAreDetected(int type) throws SQLException {
        return false;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#othersDeletesAreVisible(int)
     */
    public boolean othersDeletesAreVisible(int type) throws SQLException {
        return false;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#othersInsertsAreVisible(int)
     */
    public boolean othersInsertsAreVisible(int type) throws SQLException {
        return false;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#othersUpdatesAreVisible(int)
     */
    public boolean othersUpdatesAreVisible(int type) throws SQLException {
        return false;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#ownDeletesAreVisible(int)
     */
    public boolean ownDeletesAreVisible(int type) throws SQLException {
        return false;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#ownInsertsAreVisible(int)
     */
    public boolean ownInsertsAreVisible(int type) throws SQLException {
        return false;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#ownUpdatesAreVisible(int)
     */
    public boolean ownUpdatesAreVisible(int type) throws SQLException {
        return false;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#supportsResultSetHoldability(int)
     */
    public boolean supportsResultSetHoldability(int holdability)
            throws SQLException {
        return false;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#supportsResultSetType(int)
     */
    public boolean supportsResultSetType(int type) throws SQLException {
        return (ResultSet.TYPE_FORWARD_ONLY == type);
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#supportsTransactionIsolationLevel(int)
     */
    public boolean supportsTransactionIsolationLevel(int level)
            throws SQLException {
        return (Connection.TRANSACTION_READ_UNCOMMITTED == level);
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#updatesAreDetected(int)
     */
    public boolean updatesAreDetected(int type) throws SQLException {
        return false;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#supportsConvert(int, int)
     */
    public boolean supportsConvert(int fromType, int toType)
            throws SQLException {
        return false;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#supportsResultSetConcurrency(int, int)
     */
    public boolean supportsResultSetConcurrency(int type, int concurrency)
            throws SQLException {
        return (ResultSet.TYPE_FORWARD_ONLY == type &&
                ResultSet.CONCUR_READ_ONLY == concurrency);
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#getCatalogSeparator()
     */
    public String getCatalogSeparator() throws SQLException {
        return "";
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#getCatalogTerm()
     */
    public String getCatalogTerm() throws SQLException {
        return "Catalog";
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#getDatabaseProductName()
     */
    public String getDatabaseProductName() throws SQLException {
        return PRODUCT_NAME;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#getDatabaseProductVersion()
     */
    public String getDatabaseProductVersion() throws SQLException {
        return ("" + getDatabaseMajorVersion() + "." + getDatabaseMinorVersion());
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#getDriverName()
     */
    public String getDriverName() throws SQLException {
        return GDriver.DRIVER_NAME;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#getDriverVersion()
     */
    public String getDriverVersion() throws SQLException {
        return ("" + GDriver.MAJOR_VERSION + "." + GDriver.MINOR_VERSION);
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#getExtraNameCharacters()
     */
    public String getExtraNameCharacters() throws SQLException {
        return "";
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#getIdentifierQuoteString()
     */
    public String getIdentifierQuoteString() throws SQLException {
        return "'";
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#getNumericFunctions()
     */
    public String getNumericFunctions() throws SQLException {
        return "";
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#getProcedureTerm()
     */
    public String getProcedureTerm() throws SQLException {
        return "Procedure";
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#getSQLKeywords()
     */
    public String getSQLKeywords() throws SQLException {
        return "";
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#getSchemaTerm()
     */
    public String getSchemaTerm() throws SQLException {
        return "Schema";
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#getSearchStringEscape()
     */
    public String getSearchStringEscape() throws SQLException {
        return "";
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#getStringFunctions()
     */
    public String getStringFunctions() throws SQLException {
        return "";
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#getSystemFunctions()
     */
    public String getSystemFunctions() throws SQLException {
        return "";
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#getTimeDateFunctions()
     */
    public String getTimeDateFunctions() throws SQLException {
        return "";
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#getURL()
     */
    public String getURL() throws SQLException {
        return connection.getUrl();
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#getUserName()
     */
    public String getUserName() throws SQLException {
        return "";
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#getConnection()
     */
    public Connection getConnection() throws SQLException {
        return connection;
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#getCatalogs()
     */
    public ResultSet getCatalogs() throws SQLException {
        return new GResultSet(null, null);
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#getSchemas()
     */
    public ResultSet getSchemas() throws SQLException {
        return new GResultSet(null, null);
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#getTableTypes()
     */
    public ResultSet getTableTypes() throws SQLException {
        ResultEntry entry = new ResultEntry();
        entry.setFieldNames(new String[]{"TABLE_TYPE"});
        entry.setFieldValues(new Object[][]{{"TABLE"}});
        return new GResultSet(null, entry);
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#getTypeInfo()
     */
    public ResultSet getTypeInfo() throws SQLException {
        return new GResultSet(null, null);
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#getExportedKeys(java.lang.String, java.lang.String, java.lang.String)
     */
    public ResultSet getExportedKeys(String catalog, String schema, String table)
            throws SQLException {
        return new GResultSet(null, null);
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#getImportedKeys(java.lang.String, java.lang.String, java.lang.String)
     */
    public ResultSet getImportedKeys(String catalog, String schema, String table)
            throws SQLException {
        return new GResultSet(null, null);
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#getPrimaryKeys(java.lang.String, java.lang.String, java.lang.String)
     */
    public ResultSet getPrimaryKeys(String catalog, String schema, String table)
            throws SQLException {
        return new GResultSet(null, null);
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#getProcedures(java.lang.String, java.lang.String, java.lang.String)
     */
    public ResultSet getProcedures(String catalog, String schemaPattern,
                                   String procedureNamePattern) throws SQLException {
        throw new SQLException("Not Supported!");
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#getSuperTables(java.lang.String, java.lang.String, java.lang.String)
     */
    public ResultSet getSuperTables(String catalog, String schemaPattern,
                                    String tableNamePattern) throws SQLException {
        return new GResultSet(null, null);
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#getSuperTypes(java.lang.String, java.lang.String, java.lang.String)
     */
    public ResultSet getSuperTypes(String catalog, String schemaPattern,
                                   String typeNamePattern) throws SQLException {
        return new GResultSet(null, null);
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#getTablePrivileges(java.lang.String, java.lang.String, java.lang.String)
     */
    public ResultSet getTablePrivileges(String catalog, String schemaPattern,
                                        String tableNamePattern) throws SQLException {
        return new GResultSet(null, null);
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#getVersionColumns(java.lang.String, java.lang.String, java.lang.String)
     */
    public ResultSet getVersionColumns(String catalog, String schema,
                                       String table) throws SQLException {
        return new GResultSet(null, null);
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#getBestRowIdentifier(java.lang.String, java.lang.String, java.lang.String, int, boolean)
     */
    public ResultSet getBestRowIdentifier(String catalog, String schema,
                                          String table, int scope, boolean nullable) throws SQLException {
        return new GResultSet(null, null);
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#getIndexInfo(java.lang.String, java.lang.String, java.lang.String, boolean, boolean)
     */
    public ResultSet getIndexInfo(String catalog, String schema, String table,
                                  boolean unique, boolean approximate) throws SQLException {
        return new GResultSet(null, null);
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#getUDTs(java.lang.String, java.lang.String, java.lang.String, int[])
     */
    public ResultSet getUDTs(String catalog, String schemaPattern,
                             String typeNamePattern, int[] types) throws SQLException {
        return new GResultSet(null, null);
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#getAttributes(java.lang.String, java.lang.String, java.lang.String, java.lang.String)
     */
    public ResultSet getAttributes(String catalog, String schemaPattern,
                                   String typeNamePattern, String attributeNamePattern)
            throws SQLException {
        throw new SQLException("Not Supported!");
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#getColumnPrivileges(java.lang.String, java.lang.String, java.lang.String, java.lang.String)
     */
    public ResultSet getColumnPrivileges(String catalog, String schema,
                                         String table, String columnNamePattern) throws SQLException {
        return new GResultSet(null, null);
    }

    /**
     * Retrieves a description of table columns available in the specified catalog.
     *
     * Information is always retrieved only by the provided tableNamePattern and therefore
     * tableNamePattern cannot be null and columnNamePattern must be null. catalog & schemaPattern
     * are ignored.
     */
    public ResultSet getColumns(String catalog, String schemaPattern,
                                String tableNamePattern, String columnNamePattern)
            throws SQLException {

        if (tableNamePattern == null)
            throw new SQLException("Null table name pattern is not supported!");
        if (columnNamePattern != null)
            throw new SQLException("Not null column name pattern is not supported!");

        return connection.getTableColumnsInformation(tableNamePattern);
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#getProcedureColumns(java.lang.String, java.lang.String, java.lang.String, java.lang.String)
     */
    public ResultSet getProcedureColumns(String catalog, String schemaPattern,
                                         String procedureNamePattern, String columnNamePattern)
            throws SQLException {
        throw new SQLException("Not Supported!");
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#getTables(java.lang.String, java.lang.String, java.lang.String, java.lang.String[])
     */
    public ResultSet getTables(String catalog, String schemaPattern,
                               String tableNamePattern, String[] types) throws SQLException {
        //in the meantime...
        return new GResultSet(null, null);
    }

    /* (non-Javadoc)
     * @see java.sql.DatabaseMetaData#getCrossReference(java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.String)
     */
    public ResultSet getCrossReference(String primaryCatalog,
                                       String primarySchema, String primaryTable, String foreignCatalog,
                                       String foreignSchema, String foreignTable) throws SQLException {
        return new GResultSet(null, null);
    }

    public static void main(String[] args) {
    }

    public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public ResultSet getClientInfoProperties() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public ResultSet getFunctionColumns(String catalog, String schemaPattern,
                                        String functionNamePattern, String columnNamePattern)
            throws SQLException {
        throw new UnsupportedOperationException();
    }

    public ResultSet getFunctions(String catalog, String schemaPattern,
                                  String functionNamePattern) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public ResultSet getSchemas(String catalog, String schemaPattern)
            throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public RowIdLifetime getRowIdLifetime() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public ResultSet getPseudoColumns(String catalog, String schemaPattern,
                                      String tableNamePattern, String columnNamePattern)
            throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean generatedKeyAlwaysReturned() throws SQLException {
        throw new UnsupportedOperationException();
    }
}

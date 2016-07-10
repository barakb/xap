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


package com.gigaspaces.query;


/**
 * Class to encapsulate information of a query based on multiple Space IDs.
 *
 * An IDs query is composed of a type (provided either as a java class or as a string which contains
 * the type name) and a collection of ID values, and optionally routing value(s) an a query result
 * type indicator.
 *
 * @author Niv Ingberg
 * @see com.gigaspaces.query.QueryResultType
 * @see com.gigaspaces.query.IdQuery
 * @since 8.0
 */

public class IdsQuery<T> implements ISpaceQuery<T> {
    private final String _typeName;
    private final Object[] _ids;
    private final Object _routing;
    private final Object[] _routings;
    private final QueryResultType _queryResultType;
    private String[] _projections;

    /**
     * Creates an IdsQuery using the specified type and IDs.
     *
     * @param type Type to query.
     * @param ids  IDs to match.
     */
    public IdsQuery(Class<T> type, Object[] ids) {
        this(type.getName(), ids, null, null, QueryResultType.DEFAULT);
    }

    /**
     * Creates an IdsQuery using the specified type, IDs and query result type.
     *
     * @param type            Type to query.
     * @param ids             IDs to match.
     * @param queryResultType Type of result.
     */
    public IdsQuery(Class<T> type, Object[] ids, QueryResultType queryResultType) {
        this(type.getName(), ids, null, null, queryResultType);
    }

    /**
     * Creates an IdsQuery using the specified type, IDs and routing.
     *
     * @param type    Type to query.
     * @param ids     IDs to match.
     * @param routing Routing value to determine which partition to query.
     */
    public IdsQuery(Class<T> type, Object[] ids, Object routing) {
        this(type.getName(), ids, routing, null, QueryResultType.DEFAULT);
    }

    /**
     * Creates an IdsQuery using the specified type, IDs, routing and query result type.
     *
     * @param type            Type to query.
     * @param ids             IDs to match.
     * @param routing         Routing value to determine which partition to query.
     * @param queryResultType Type of result.
     */
    public IdsQuery(Class<T> type, Object[] ids, Object routing, QueryResultType queryResultType) {
        this(type.getName(), ids, routing, null, queryResultType);
    }

    /**
     * Creates an IdsQuery using the specified type, IDs and routing per id.
     *
     * @param type     Type to query.
     * @param ids      IDs to match.
     * @param routings Routing values (per id) to determine which partition to query.
     */
    public IdsQuery(Class<T> type, Object[] ids, Object[] routings) {
        this(type.getName(), ids, null, routings, QueryResultType.DEFAULT);
    }

    /**
     * Creates an IdsQuery using the specified type, IDs, routing per id and query result type.
     *
     * @param type            Type to query.
     * @param ids             IDs to match.
     * @param routings        Routing values (per id) to determine which partition to query.
     * @param queryResultType Type of result.
     */
    public IdsQuery(Class<T> type, Object[] ids, Object[] routings, QueryResultType queryResultType) {
        this(type.getName(), ids, null, routings, queryResultType);
    }

    /**
     * Creates an IdsQuery using the specified type and IDs.
     *
     * @param typeName Type to query.
     * @param ids      IDs to match.
     */
    public IdsQuery(String typeName, Object[] ids) {
        this(typeName, ids, null, null, QueryResultType.DEFAULT);
    }

    /**
     * Creates an IdsQuery using the specified type, IDs and query result type.
     *
     * @param typeName        Type to query.
     * @param ids             IDs to match.
     * @param queryResultType Type of result.
     */
    public IdsQuery(String typeName, Object[] ids, QueryResultType queryResultType) {
        this(typeName, ids, null, null, queryResultType);
    }

    /**
     * Creates an IdsQuery using the specified type, IDs and routing.
     *
     * @param typeName Type to query.
     * @param ids      IDs to match.
     * @param routing  Routing value to determine which partition to query.
     */
    public IdsQuery(String typeName, Object[] ids, Object routing) {
        this(typeName, ids, routing, null, QueryResultType.DEFAULT);
    }

    /**
     * Creates an IdsQuery using the specified type, IDs, routing and query result type.
     *
     * @param typeName        Type to query.
     * @param ids             IDs to match.
     * @param routing         Routing value to determine which partition to query.
     * @param queryResultType Type of result.
     */
    public IdsQuery(String typeName, Object[] ids, Object routing, QueryResultType queryResultType) {
        this(typeName, ids, routing, null, queryResultType);
    }

    /**
     * Creates an IdsQuery using the specified type, IDs and routing per id.
     *
     * @param typeName Type to query.
     * @param ids      IDs to match.
     * @param routings Routing values (per id) to determine which partition to query.
     */
    public IdsQuery(String typeName, Object[] ids, Object[] routings) {
        this(typeName, ids, null, routings, QueryResultType.DEFAULT);
    }

    /**
     * Creates an IdsQuery using the specified type, IDs, routing per id and query result type.
     *
     * @param typeName        Type to query.
     * @param ids             IDs to match.
     * @param routings        Routing values (per id) to determine which partition to query.
     * @param queryResultType Type of result.
     */
    public IdsQuery(String typeName, Object[] ids, Object[] routings, QueryResultType queryResultType) {
        this(typeName, ids, null, routings, queryResultType);
    }

    private IdsQuery(String typeName, Object[] ids, Object routing, Object[] routings, QueryResultType queryResultType) {
        this._typeName = typeName;
        this._ids = ids;
        this._routing = routing;
        this._routings = routings;
        this._queryResultType = queryResultType;
    }

    /**
     * Returns the name of type to query.
     */
    public String getTypeName() {
        return _typeName;
    }

    /**
     * Returns the IDs to match.
     */
    public Object[] getIds() {
        return _ids;
    }

    /**
     * Returns the routing value used to determine the partition to query.
     */
    public Object getRouting() {
        return _routing;
    }

    /**
     * Returns the routing values used to determine the partition to query.
     */
    public Object[] getRoutings() {
        return _routings;
    }

    /**
     * Returns the type of result.
     */
    public QueryResultType getQueryResultType() {
        return _queryResultType;
    }

    /**
     * Sets projection properties which specifies that a result for an operation using this query
     * should contain data for the specified projection properties. Other properties which were not
     * specifies as a projection will not contain data. By default, if no projection was added, all
     * the properties are returned with full data.
     */
    public IdsQuery<T> setProjections(String... properties) {
        _projections = properties;
        return this;
    }

    /**
     * Returns the projections set on this query, null means no projection is used and all the
     * properties should be returned.
     */
    public String[] getProjections() {
        return _projections;
    }
}

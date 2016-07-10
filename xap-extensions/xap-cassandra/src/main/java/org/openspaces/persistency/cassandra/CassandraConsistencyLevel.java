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

package org.openspaces.persistency.cassandra;

/**
 * Representing consistency level for read/write operations.
 *
 * @author Dan Kilman
 * @see <a href="http://www.datastax.com/docs/1.1/references/cql/cql_data_types">
 * http://www.datastax.com/docs/1.1/references/cql/cql_data_types</a>
 * @since 9.1.1
 */

public enum CassandraConsistencyLevel {

    ONE,

    QUORUM,

    ALL,

    /* not supported for read operations */
    ANY,

    /* when using multiple data centers */
    EACH_QUORUM,

    /* when using multiple data centers */
    LOCAL_QUORUM;

}

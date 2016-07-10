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

package org.openspaces.persistency.cassandra.pool;

import com.j_spaces.kernel.pool.IResourceFactory;

import org.openspaces.persistency.cassandra.error.SpaceCassandraDataSourceException;

import java.sql.SQLException;

import javax.sql.DataSource;

/**
 * A {@link IResourceFactory} for obtaining {@link ConnectionResource} instances. Uses a {@link
 * javax.sql.DataSource} as the underlying method of obtaining new connections.
 *
 * @author Dan Kilman
 * @since 9.1.1
 */
public class ConnectionResourceFactory
        implements IResourceFactory<ConnectionResource> {

    private final DataSource dataSource;

    public ConnectionResourceFactory(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public ConnectionResource allocate() {
        try {
            return new ConnectionResource(dataSource.getConnection(), dataSource);
        } catch (SQLException e) {
            throw new SpaceCassandraDataSourceException("Could not allocate a new connection", e);
        }
    }
}

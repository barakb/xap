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

import com.j_spaces.kernel.pool.IResource;
import com.j_spaces.kernel.pool.Resource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openspaces.persistency.cassandra.error.SpaceCassandraDataSourceException;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

/**
 * A {@link IResource} representing a {@link java.sql.Connection}.
 *
 * @author Dan Kilman
 * @since 9.1.1
 */
public class ConnectionResource extends Resource
        implements IResource {

    private static final Log logger = LogFactory.getLog(ConnectionResource.class);

    /**
     * We use the data source to create a new underlying connection when some exception occures that
     * requires a need connection to be created. this will will only happen for connections that are
     * part of the pool, because connections that are not part of the pool will be closed when the
     * resource is cleared.
     */
    private final DataSource dataSource;

    /*
     * Intentionally left unvolatile. worst case scenario, the user will get a connection exception
     */
    private boolean closed;

    private Connection connection;

    public ConnectionResource(Connection connection, DataSource dataSource) {
        this.connection = connection;
        this.dataSource = dataSource;
    }

    @Override
    public void clear() {
        if (!isFromPool()) {
            closeUnderlyingConnection();
        }
    }

    /**
     * @return The underlying {@link java.sql.Connection} represented by this {@link
     * ConnectionResource}
     */
    public Connection getConnection() {
        if (closed) {
            throw new SpaceCassandraDataSourceException("Resource already closed");
        }

        if (connection == null) {
            try {
                connection = dataSource.getConnection();
            } catch (SQLException e) {
                throw new SpaceCassandraDataSourceException("Could not create a new connection", e);
            }
        }
        return connection;
    }

    /**
     * Must be called by the resource owner
     */
    public void closeCurrentConnection() {
        if (connection != null) {
            closeUnderlyingConnection();
            connection = null;
        }
    }

    private void closeUnderlyingConnection() {
        try {
            connection.close();
        } catch (SQLException e) {
            if (logger.isWarnEnabled()) {
                logger.warn("Failed closing jdbc connection", e);
            }
        }
    }

    public void close() {
        closeUnderlyingConnection();
        closed = true;
    }
}

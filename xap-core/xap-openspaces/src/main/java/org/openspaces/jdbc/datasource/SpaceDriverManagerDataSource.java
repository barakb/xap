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


package org.openspaces.jdbc.datasource;

import com.j_spaces.core.IJSpace;
import com.j_spaces.jdbc.driver.GConnection;

import org.openspaces.core.GigaSpace;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.jdbc.datasource.AbstractDataSource;
import org.springframework.util.Assert;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * A simple Jdbc {@link javax.sql.DataSource} based on a {@link com.j_spaces.core.IJSpace space}.
 * Returns a new Jdbc {@link java.sql.Connection} for each <code>getConnection</code>.
 *
 * @author kimchy
 */
public class SpaceDriverManagerDataSource extends AbstractDataSource implements InitializingBean {

    private IJSpace space;

    public SpaceDriverManagerDataSource() {

    }

    public SpaceDriverManagerDataSource(IJSpace space) {
        this.space = space;
    }

    public void setSpace(IJSpace space) {
        this.space = space;
    }

    public void setGigaSpace(GigaSpace gigaSpace) {
        this.space = gigaSpace.getSpace();
    }

    public void afterPropertiesSet() throws Exception {
        Assert.notNull(space, "space property must be set");
    }

    public Connection getConnection() throws SQLException {
        return GConnection.getInstance(space);
    }

    public Connection getConnection(String username, String password) throws SQLException {
        return GConnection.getInstance(space, username, password);
    }

}

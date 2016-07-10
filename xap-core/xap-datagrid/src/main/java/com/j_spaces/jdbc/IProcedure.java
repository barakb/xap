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

import com.j_spaces.core.IJSpace;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;

/**
 * The IProcedure interface should be implemented when using the JDBC API to invoke Java classes
 * business logic at the space side. The IProcedure interface includes the execute method that
 * returns {@link ResultEntry }
 *
 * @deprecated Use {@link org.openspaces.core.executor.Task} or {@link org.openspaces.core.executor.DistributedTask}
 * instead.
 */
public interface IProcedure {
    /**
     * The execute method should include the business logic to be invoked at the server side
     *
     * @param connection JDBC Connection
     * @param space      The embedded space proxy
     * @param paramList  Parameters to pass to the stored procedure
     * @return Returns result set into the form of {@link ResultEntry}.
     * @throws {@link SQLException}
     */
    public ResultEntry execute(Connection connection, IJSpace space, ArrayList paramList) throws SQLException;
}

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

package org.openspaces.itest.persistency.cassandra;

import com.gigaspaces.logger.GSLogConfigLoader;

import org.openspaces.itest.persistency.cassandra.helper.EmbeddedCassandraController;

public class CassandraTestServer {

    private String keySpaceName = "space";
    private int rpcPort;
    private final EmbeddedCassandraController cassandraController = new EmbeddedCassandraController();

    /**
     * @param isEmbedded - run Cassandra in this process. Use for debugging only since causes
     *                   leaks.
     */
    public void initialize(boolean isEmbedded) {
        if (CassandraTestSuite.isSuiteMode()) {
            keySpaceName = CassandraTestSuite.createKeySpaceAndReturnItsName();
            rpcPort = CassandraTestSuite.getRpcPort();
        } else {
            GSLogConfigLoader.getLoader();
            cassandraController.initCassandra(isEmbedded);
            cassandraController.createKeySpace(keySpaceName);
            rpcPort = cassandraController.getRpcPort();
        }
    }

    public void destroy() {
        if (!CassandraTestSuite.isSuiteMode()) {
            cassandraController.stopCassandra();
        }
    }

    public int getPort() {
        return rpcPort;
    }

    public String getKeySpaceName() {
        return keySpaceName;
    }

    public String getHost() {
        return "localhost";
    }
}

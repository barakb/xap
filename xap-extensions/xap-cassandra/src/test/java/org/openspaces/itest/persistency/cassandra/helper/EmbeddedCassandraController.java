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

package org.openspaces.itest.persistency.cassandra.helper;

import org.github.jamm.MemoryMeter;
import org.jini.rio.boot.BootUtil;
import org.openspaces.itest.persistency.cassandra.helper.config.CassandraConfigUtils;
import org.openspaces.test.client.executor.Executor;
import org.openspaces.test.client.executor.ExecutorUtils;
import org.openspaces.test.client.executor.RemoteAsyncCommandResult;
import org.openspaces.test.client.executor.RemoteJavaCommand;

import java.io.File;
import java.net.URL;
import java.util.UUID;


public class EmbeddedCassandraController {

    private static final String CASSANDRA_YAML = "org/openspaces/itest/persistency/cassandra/cassandra.yaml";

    private RemoteAsyncCommandResult<IEmbeddedCassandra> _dbRemoteAdmin;
    private IEmbeddedCassandra _db;
    private int _rpcPort = EmbeddedCassandra.DEFAULT_RPC_PORT;

    public void initCassandra(boolean isEmbedded) {
        if (isEmbedded) {
            System.setProperty("cassandra.config", CASSANDRA_YAML);
            _db = new EmbeddedCassandra();
        } else {
            try {
                URL baseConfiguration = Thread
                        .currentThread()
                        .getContextClassLoader()
                        .getResource(CASSANDRA_YAML);
                String randomUID = UUID.randomUUID().toString().substring(0, 8);
                File configDestination = new File("target/cassandra-" + randomUID + ".yaml");
                String root = "target/cassandra/" + randomUID;
                CassandraConfigUtils.writeCassandraConfig(baseConfiguration, configDestination, root);

                String rpcPort = BootUtil.getAnonymousPort();
                _rpcPort = Integer.parseInt(rpcPort);

                RemoteJavaCommand<IEmbeddedCassandra> cmd =
                        new RemoteJavaCommand<IEmbeddedCassandra>(EmbeddedCassandra.class, null);
                cmd.addJVMArg("-ea");
                cmd.addJVMArg("-javaagent:" + wrapWithQuotesIfWindows(
                        new File(MemoryMeter.class.getProtectionDomain()
                                .getCodeSource()
                                .getLocation()
                                .getPath()).getAbsolutePath()));
                cmd.addJVMArg("-Xms1G");
                cmd.addJVMArg("-Xmx1G");
                cmd.addJVMArg("-XX:+HeapDumpOnOutOfMemoryError");
                cmd.addJVMArg("-XX:+UseParNewGC");
                cmd.addJVMArg("-XX:+UseConcMarkSweepGC");
                cmd.addJVMArg("-XX:+CMSParallelRemarkEnabled");
                cmd.addJVMArg("-XX:SurvivorRatio=8");
                cmd.addJVMArg("-XX:MaxTenuringThreshold=1");
                cmd.addJVMArg("-XX:CMSInitiatingOccupancyFraction=75");
                cmd.addJVMArg("-XX:+UseCMSInitiatingOccupancyOnly");
                cmd.addSystemPropParameter(EmbeddedCassandra.RPC_PORT_PROP, rpcPort);
                cmd.addSystemPropParameter("cassandra.config", configDestination.toURI().toString());
                cmd.addSystemPropParameter("cassandra.storage_port", BootUtil.getAnonymousPort());
                cmd.addSystemPropParameter("com.sun.management.jmxremote.port", BootUtil.getAnonymousPort());
                cmd.addSystemPropParameter("com.sun.management.jmxremote.ssl", "false");
                cmd.addSystemPropParameter("com.sun.management.jmxremote.authenticate", "false");
                _dbRemoteAdmin = Executor.executeAsync(cmd, null);
                _db = _dbRemoteAdmin.getRemoteStub();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static String wrapWithQuotesIfWindows(String string) {
        if (ExecutorUtils.isUnixOS())
            return string;

        return "\"" + string + "\"";
    }

    public void createKeySpace(String keySpaceName) {
        _db.createKeySpace(keySpaceName);
    }

    public void dropKeySpace(String keySpaceName) {
        _db.dropKeySpace(keySpaceName);
    }

    public void stopCassandra() {
        _db.destroy();
        if (_dbRemoteAdmin != null) {
            try {
                _dbRemoteAdmin.stop(true);
                while (_dbRemoteAdmin.getProcessAdmin().isAlive()) {
                    Thread.sleep(1000);
                }
            } catch (Exception e) {

            }
        }
    }

    public int getRpcPort() {
        return _rpcPort;
    }

}

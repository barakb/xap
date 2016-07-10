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

package org.openspaces.itest.persistency.cassandra.spring;

import com.gigaspaces.datasource.DataIterator;
import com.gigaspaces.document.SpaceDocument;
import com.gigaspaces.metadata.SpaceTypeDescriptorBuilder;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.openspaces.itest.persistency.cassandra.CassandraTestServer;
import org.openspaces.persistency.cassandra.CassandraConsistencyLevel;
import org.openspaces.persistency.cassandra.CassandraSpaceDataSource;
import org.openspaces.persistency.cassandra.CassandraSpaceSynchronizationEndpoint;
import org.openspaces.test.common.mock.MockIntroduceTypeData;
import org.openspaces.test.common.mock.MockOperationsBatchDataBuilder;
import org.springframework.beans.factory.config.PropertyPlaceholderConfigurer;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.util.Map.Entry;
import java.util.Properties;

public class CassandaraFactoryBeansTest {

    private final String TEST_FACTORY_XML =
            "/org/openspaces/itest/persistency/cassandra/spring/test-cassandra-factory-beans.xml";
    private final CassandraTestServer server = new CassandraTestServer();

    private CassandraSpaceDataSource dataSource;
    private CassandraSpaceSynchronizationEndpoint syncEndpoint;

    @Before
    public void startServer() {
        server.initialize(false);
    }

    @After
    public void stopServer() {
        server.destroy();
    }

    @Test
    public void test() {

        final boolean refreshNow = false;
        final ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext
                (new String[]{TEST_FACTORY_XML}, refreshNow);

        PropertyPlaceholderConfigurer propertyConfigurer = new PropertyPlaceholderConfigurer();
        Properties properties = new Properties();
        properties.setProperty("cassandra.hosts", server.getHost());
        properties.setProperty("cassandra.port", String.valueOf(server.getPort()));
        properties.setProperty("cassandra.keyspace", server.getKeySpaceName());
        properties.setProperty("cassandra.user", "default");
        properties.setProperty("cassandra.password", "default");
        properties.setProperty("cassandra.ds.cluster", "ds_cluster");
        properties.setProperty("cassandra.sync.cluster", "sync_cluster");
        properties.setProperty("cassandra.ds.minconnections", String.valueOf(1));
        properties.setProperty("cassandra.ds.maxconnections", String.valueOf(5));
        properties.setProperty("cassandra.ds.batchlimit", String.valueOf(100));
        properties.setProperty("cassandra.hector.gcgrace", String.valueOf(60 * 60 * 24 * 10));
        properties.setProperty("cassandra.hector.read.consistency.level", CassandraConsistencyLevel.QUORUM.name());
        properties.setProperty("cassandra.hector.write.consistency.level", CassandraConsistencyLevel.ONE.name());
        propertyConfigurer.setProperties(properties);
        context.addBeanFactoryPostProcessor(propertyConfigurer);
        context.refresh();

        try {
            syncEndpoint = context.getBean(CassandraSpaceSynchronizationEndpoint.class);
            dataSource = context.getBean(CassandraSpaceDataSource.class);
            doWork();
        } finally {
            context.close();
        }
    }

    private void doWork() {
        syncEndpoint.onIntroduceType(createIntroduceTypeDataFromSpaceDocument(createDocument(123), "keyName"));
        MockOperationsBatchDataBuilder builder = new MockOperationsBatchDataBuilder();
        SpaceDocument document = createDocument(111);
        builder.write(document, "keyName");
        syncEndpoint.onOperationsBatchSynchronization(builder.build());
        dataSource.initialMetadataLoad();
        DataIterator<Object> iterator = dataSource.initialDataLoad();
        Assert.assertTrue("missing result", iterator.hasNext());
        SpaceDocument result = (SpaceDocument) iterator.next();
        Assert.assertEquals("bad result", document, result);
        iterator.close();
    }

    private SpaceDocument createDocument(int key) {
        return new SpaceDocument("TypeName")
                .setProperty("keyName", key)
                .setProperty("someProp", key);
    }

    protected MockIntroduceTypeData createIntroduceTypeDataFromSpaceDocument(
            SpaceDocument document, String key) {
        SpaceTypeDescriptorBuilder builder = new SpaceTypeDescriptorBuilder(document.getTypeName());
        for (Entry<String, Object> entry : document.getProperties().entrySet())
            builder.addFixedProperty(entry.getKey(), entry.getValue().getClass());
        builder.idProperty(key);
        return new MockIntroduceTypeData(builder.create());
    }
}

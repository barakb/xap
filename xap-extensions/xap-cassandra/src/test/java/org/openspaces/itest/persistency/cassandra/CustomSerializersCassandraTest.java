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

import com.gigaspaces.datasource.DataIterator;
import com.gigaspaces.document.SpaceDocument;
import com.gigaspaces.sync.IntroduceTypeData;

import junit.framework.Assert;

import org.apache.cassandra.cql.jdbc.CassandraDataSource;
import org.junit.Before;
import org.junit.Test;
import org.openspaces.persistency.cassandra.CassandraSpaceDataSource;
import org.openspaces.persistency.cassandra.CassandraSpaceSynchronizationEndpoint;
import org.openspaces.persistency.cassandra.HectorCassandraClient;
import org.openspaces.persistency.cassandra.meta.mapping.filter.FlattenedPropertiesFilter;
import org.openspaces.persistency.cassandra.meta.mapping.filter.PropertyContext;
import org.openspaces.persistency.cassandra.meta.types.SerializerProvider;
import org.openspaces.persistency.cassandra.meta.types.dynamic.PropertyValueSerializer;
import org.openspaces.test.common.data.TestPojo1;
import org.openspaces.test.common.mock.MockDataSourceQuery;
import org.openspaces.test.common.mock.MockOperationsBatchDataBuilder;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

public class CustomSerializersCassandraTest extends AbstractCassandraTest {

    private final AtomicInteger toByteBufferHitCount = new AtomicInteger(0);
    private final AtomicInteger fromByteBufferHitCount = new AtomicInteger(0);

    private final String typeName = "TypeName";

    private final String keyName = "key";
    private final Integer keyValue = 1;

    private final String primitiveFixedPropName = "primitiveFixedProp";
    private final Integer primitiveFixedPropValue = 123123;
    private final String objectFixedPropName = "objectFixedProp";
    private final TestPojo1 objectFixedPropValue = new TestPojo1("123123");
    private final String primitiveDynamicPropName = "primitiveDynamicProp";
    private final Integer primitiveDynamicPropValue = 333333;
    private final String objectDynamicPropName = "dynProp";
    private final TestPojo1 objectDynamicPropValue = new TestPojo1("123");

    private IntroduceTypeData introduceDataType;

    @Before
    public void before() {
        introduceDataType = createIntroduceTypeDataFromSpaceDocument(createSpaceDocument(),
                keyName);
        _syncInterceptor.onIntroduceType(introduceDataType);
        _dataSource.initialMetadataLoad();
    }

    @Test
    public void test() {
        MockOperationsBatchDataBuilder builder = new MockOperationsBatchDataBuilder();
        SpaceDocument spaceDoc = createSpaceDocument()
                .setProperty(primitiveDynamicPropName, primitiveDynamicPropValue)
                .setProperty(objectDynamicPropName, objectDynamicPropValue);
        builder.write(spaceDoc, keyName);
        _syncInterceptor.onOperationsBatchSynchronization(builder.build());


        DataIterator<Object> iterator = _dataSource.getDataIterator(new MockDataSourceQuery(introduceDataType.getTypeDescriptor(),
                new SpaceDocument(typeName),
                Integer.MAX_VALUE));

        Assert.assertTrue("No object found", iterator.hasNext());
        SpaceDocument result = (SpaceDocument) iterator.next();
        Assert.assertEquals("Wrong type name", typeName, result.getTypeName());
        Assert.assertEquals("Wrong value", keyValue, result.getProperty(keyName));

        Assert.assertEquals("Wrong value", primitiveFixedPropValue, result.getProperty(primitiveFixedPropName));
        Assert.assertEquals("Wrong value", objectFixedPropValue, result.getProperty(objectFixedPropName));
        Assert.assertEquals("Wrong value", primitiveDynamicPropValue, result.getProperty(primitiveDynamicPropName));
        Assert.assertEquals("Wrong value", objectDynamicPropValue, result.getProperty(objectDynamicPropName));

        // we used the same custom serializer for both fixed and dynamic properties
        // we also requested that no flattening should be performed 
        // so we expected the custom serializer to be used 3 times
        // (the primitive fixed value is serialized using standard serialization)
        Assert.assertEquals(3, toByteBufferHitCount.get());
        Assert.assertEquals(3, fromByteBufferHitCount.get());

        iterator.close();
    }

    private SpaceDocument createSpaceDocument() {
        return new SpaceDocument(typeName)
                .setProperty(keyName, 1)
                .setProperty(primitiveFixedPropName, primitiveFixedPropValue)
                .setProperty(objectFixedPropName, objectFixedPropValue);
    }

    private final FlattenedPropertiesFilter _simpleFilter = new FlattenedPropertiesFilter() {
        public boolean shouldFlatten(PropertyContext context) {
            return false;
        }
    };

    private final PropertyValueSerializer _simpleSerializer = new PropertyValueSerializer() {
        public ByteBuffer toByteBuffer(Object obj) {
            toByteBufferHitCount.incrementAndGet();
            return SerializerProvider.getObjectSerializer().toByteBuffer(obj);
        }

        public Object fromByteBuffer(ByteBuffer byteBuffer) {
            fromByteBufferHitCount.incrementAndGet();
            return SerializerProvider.getObjectSerializer().fromByteBuffer(byteBuffer);
        }
    };

    @Override
    protected CassandraSpaceSynchronizationEndpoint createCassandraSyncEndpointInterceptor(
            HectorCassandraClient hectorClient) {
        CassandraSpaceSynchronizationEndpoint syncInterceptor =
                new CassandraSpaceSynchronizationEndpoint(_simpleSerializer,
                        _simpleSerializer,
                        _simpleFilter,
                        null,
                        hectorClient);
        return syncInterceptor;
    }

    @Override
    protected CassandraSpaceDataSource createCassandraSpaceDataSource(
            HectorCassandraClient hectorClient) {
        CassandraDataSource ds = createCassandraDataSource();
        CassandraSpaceDataSource dataSource = new CassandraSpaceDataSource(_simpleSerializer,
                _simpleSerializer,
                ds,
                hectorClient,
                5,
                30,
                10000, null, true, null);
        return dataSource;
    }

}

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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.openspaces.test.common.mock.MockOperationsBatchDataBuilder;

import java.util.HashSet;
import java.util.Set;

public class WriteAndRemoveCassandraTest extends AbstractCassandraTest {

    private final String typeName = "MyType";
    private final String keyName = "keyName";

    @Before
    public void before() {
        _syncInterceptor.onIntroduceType(createIntroduceTypeDataFromSpaceDocument(createSpaceDocument(0),
                keyName));
        _dataSource.initialMetadataLoad();
    }

    @Test
    public void test() {
        MockOperationsBatchDataBuilder builder = new MockOperationsBatchDataBuilder();
        builder.write(createSpaceDocument(1), keyName);
        builder.write(createSpaceDocument(2), keyName);
        builder.write(createSpaceDocument(3), keyName);
        _syncInterceptor.onOperationsBatchSynchronization(builder.build());

        assertValidData(1, 2, 3);

        builder.clear();
        builder.remove(createSpaceDocument(1), keyName);
        _syncInterceptor.onOperationsBatchSynchronization(builder.build());

        assertValidData(2, 3);

        builder.clear();
        builder.write(createSpaceDocument(1), keyName);
        builder.write(createSpaceDocument(2), keyName);
        builder.write(createSpaceDocument(3), keyName);
        builder.write(createSpaceDocument(4), keyName);
        _syncInterceptor.onOperationsBatchSynchronization(builder.build());

        assertValidData(1, 2, 3, 4);

        builder.clear();
        builder.write(createSpaceDocument(1), keyName);
        builder.remove(createSpaceDocument(2), keyName);
        builder.write(createSpaceDocument(3), keyName);
        builder.write(createSpaceDocument(5), keyName);
        builder.remove(createSpaceDocument(6), keyName);
        _syncInterceptor.onOperationsBatchSynchronization(builder.build());

        assertValidData(1, 3, 4, 5);

        builder.clear();
    }

    private void assertValidData(long... expectedKeys) {
        Set<Long> expected = new HashSet<Long>();
        for (long num : expectedKeys)
            expected.add(num);

        DataIterator<Object> iterator = _dataSource.initialDataLoad();
        Set<Long> read = new HashSet<Long>();
        while (iterator.hasNext()) {
            SpaceDocument doc = (SpaceDocument) iterator.next();
            Long key = doc.getProperty(keyName);
            read.add(key);
            Assert.assertEquals("unexpected property", key, doc.getProperty("some_prop"));
        }

        iterator.close();

        Assert.assertEquals("Unexpected set", expected, read);
    }

    private SpaceDocument createSpaceDocument(long key) {
        return new SpaceDocument(typeName)
                .setProperty(keyName, key)
                .setProperty("some_prop", key);
    }

}

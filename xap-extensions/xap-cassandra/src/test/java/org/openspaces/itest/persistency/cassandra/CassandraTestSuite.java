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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;
import org.openspaces.itest.persistency.cassandra.helper.EmbeddedCassandraController;

import java.util.concurrent.atomic.AtomicInteger;


@RunWith(Suite.class)
@SuiteClasses(value =
        {
                ConcurrentColumnFamilyCreationCassandraTest.class,
                DataIteratorWithPropertyAddedLaterCassandraTest.class,
                ReadByIdWithPropertyAddedLaterCassandraTest.class,
                ReadByIdsCassandraTest.class,
                PojoWithPrimitiveTypesCassandraTest.class,
                DifferentTypesQueryCassandraTest.class,
                BasicCQLQueriesCassandraTest.class,
                VeryLongTypeNameCassandraTest.class,
                WriteAndRemoveCassandraTest.class,
                CyclicReferencePropertyCassandraTest.class,
                ColumnFamilyMetadataSpaceTypeDescriptorConversionTest.class,
                BasicCassandraTest.class,
                CustomSerializersCassandraTest.class,
                MultiTypeCassandraTest.class,
                MultiTypeNestedPropertiesCassandraTest.class,
                DifferentConsistencyLevelsCassandraTest.class,
                InitialDataLoadCassandraTest.class,
                DifferentConsistencyLevelsCassandraTest.class
        })
public class CassandraTestSuite {
    private static final AtomicInteger runningNumber = new AtomicInteger(0);
    private static volatile boolean isSuiteMode = false;

    private static final EmbeddedCassandraController cassandraController = new EmbeddedCassandraController();

    @BeforeClass
    public static void beforeSuite() {
        GSLogConfigLoader.getLoader();
        isSuiteMode = true;
        cassandraController.initCassandra(false);
    }

    @AfterClass
    public static void afterSuite() {
        isSuiteMode = false;
        cassandraController.stopCassandra();
    }

    public static String createKeySpaceAndReturnItsName() {
        String keySpaceName = "space" + runningNumber.incrementAndGet();
        cassandraController.createKeySpace(keySpaceName);
        return keySpaceName;
    }

    public static void dropKeySpace(String keySpaceName) {
        cassandraController.dropKeySpace(keySpaceName);
    }

    public static boolean isSuiteMode() {
        return isSuiteMode;
    }

    public static int getRpcPort() {
        return cassandraController.getRpcPort();
    }

}

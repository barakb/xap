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

package org.openspaces.test.common;

import com.gigaspaces.metadata.SpaceTypeDescriptor;
import com.gigaspaces.metadata.index.SpaceIndex;

import junit.framework.Assert;

public class TestSpaceTypeDescriptorUtils {
    public static void assertTypeDescriptorsEquals(SpaceTypeDescriptor expected, SpaceTypeDescriptor actual) {
        Assert.assertEquals(expected.getTypeName(), actual.getTypeName());
        Assert.assertEquals(expected.getSuperTypeName(), actual.getSuperTypeName());
        Assert.assertEquals(expected.isAutoGenerateId(), actual.isAutoGenerateId());
        Assert.assertEquals(expected.getTypeSimpleName(), actual.getTypeSimpleName());
        Assert.assertEquals(expected.getFifoGroupingPropertyPath(), actual.getFifoGroupingPropertyPath());
        Assert.assertEquals(expected.getIdPropertyName(), actual.getIdPropertyName());
        Assert.assertEquals(expected.getNumOfFixedProperties(), actual.getNumOfFixedProperties());
        Assert.assertEquals(expected.getRoutingPropertyName(), actual.getRoutingPropertyName());
        Assert.assertEquals(expected.isConcreteType(), actual.isConcreteType());
        Assert.assertEquals(expected.isReplicable(), actual.isReplicable());
        Assert.assertEquals(expected.getDocumentWrapperClass(), actual.getDocumentWrapperClass());
        Assert.assertEquals(expected.getFifoGroupingIndexesPaths(), actual.getFifoGroupingIndexesPaths());
        Assert.assertEquals(expected.getFifoSupport(), actual.getFifoSupport());
        Assert.assertEquals(expected.supportsDynamicProperties(), actual.supportsDynamicProperties());
        Assert.assertEquals(expected.supportsOptimisticLocking(), actual.supportsOptimisticLocking());
        Assert.assertEquals(expected.getObjectClass(), actual.getObjectClass());
        Assert.assertEquals(expected.getStorageType(), actual.getStorageType());
        for (int i = 0; i < expected.getNumOfFixedProperties(); i++) {
            Assert.assertEquals(expected.getFixedProperty(i).getName(), actual.getFixedProperty(i).getName());
            Assert.assertEquals(expected.getFixedProperty(i).getTypeName(), actual.getFixedProperty(i).getTypeName());
            Assert.assertEquals(expected.getFixedProperty(i).getTypeDisplayName(), actual.getFixedProperty(i).getTypeDisplayName());
            Assert.assertEquals(expected.getFixedProperty(i).getDocumentSupport(), actual.getFixedProperty(i).getDocumentSupport());
            Assert.assertEquals(expected.getFixedProperty(i).getType(), actual.getFixedProperty(i).getType());
            Assert.assertEquals(expected.getFixedProperty(i).getStorageType(), actual.getFixedProperty(i).getStorageType());
        }

        for (SpaceIndex expectedSpaceIndex : expected.getIndexes().values()) {
            SpaceIndex actualSpaceIndex = actual.getIndexes().get(expectedSpaceIndex.getName());
            Assert.assertEquals(expectedSpaceIndex.getName(), actualSpaceIndex.getName());
            Assert.assertEquals(expectedSpaceIndex.getIndexType(), actualSpaceIndex.getIndexType());
        }

    }
}

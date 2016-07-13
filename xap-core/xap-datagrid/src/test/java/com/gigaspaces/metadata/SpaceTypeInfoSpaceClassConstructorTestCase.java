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
package com.gigaspaces.metadata;

import com.gigaspaces.internal.metadata.SpacePropertyInfo;
import com.gigaspaces.internal.metadata.SpaceTypeInfo;
import com.gigaspaces.internal.metadata.SpaceTypeInfoRepository;
import com.gigaspaces.metadata.pojos.PojoExtendsWithSpaceClassConstructor;
import com.gigaspaces.metadata.pojos.PojoWithFieldInConstructorAndHasSetter;
import com.gigaspaces.metadata.pojos.PojoWithSpaceClassConstructorAndSetter;

import junit.framework.Assert;
import junit.framework.TestCase;

/**
 * @author: yaeln
 * @since: 12.0.0
 */

public class SpaceTypeInfoSpaceClassConstructorTestCase extends TestCase {

    public void testPojoWithSpaceClassConstructorAndSetter() {

        final Class<?> type = PojoWithSpaceClassConstructorAndSetter.class;

        SpaceTypeInfo typeInfo = SpaceTypeInfoRepository.getTypeInfo(type);
        SpacePropertyInfo[] spaceProperties = typeInfo.getSpaceProperties();
        Assert.assertEquals("Pojo doesn't contain 2 space properties", 2, spaceProperties.length);
        Assert.assertEquals("first property is not 'id'", "id", spaceProperties[0].getName());
        Assert.assertEquals("second property is not 'name'", "name", spaceProperties[1].getName());
    }

    public void testPojoExtendsWithSpaceClassConstructor() {

        final Class<?> type = PojoExtendsWithSpaceClassConstructor.class;

        SpaceTypeInfo typeInfo = SpaceTypeInfoRepository.getTypeInfo(type);
        SpacePropertyInfo[] spaceProperties = typeInfo.getSpaceProperties();
        Assert.assertEquals("Pojo doesn't contain 3 space properties", 3, spaceProperties.length);
        Assert.assertEquals("first property is not 'metaId'", "metaId", spaceProperties[0].getName());
        Assert.assertEquals("second property is not 'id'", "id", spaceProperties[1].getName());
        Assert.assertEquals("third property is not 'name'", "name", spaceProperties[2].getName());
    }

    public void testPojoWithFieldInConstructorAndHasSetter() {

        final Class<?> type = PojoWithFieldInConstructorAndHasSetter.class;
        try {
            SpaceTypeInfoRepository.getTypeInfo(type);
        } catch (Exception e) {
            Assert.assertTrue("Should throw SpaceMetadataValidationException but didn't", e instanceof SpaceMetadataValidationException);
        }
    }

}

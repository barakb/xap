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

package org.openspaces.utest.persistency.cassandra.meta.mapping;

import com.gigaspaces.annotation.pojo.SpaceId;
import com.gigaspaces.metadata.SpaceTypeDescriptor;
import com.gigaspaces.metadata.SpaceTypeDescriptorBuilder;

import org.junit.Assert;
import org.junit.Test;
import org.openspaces.persistency.support.SpaceTypeDescriptorContainer;
import org.openspaces.persistency.support.TypeDescriptorUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TypeHierarcyTopologySorterTest {

    @Test
    public void test() {
        testHierarchy1();

        testHierarchy2();
    }

    private void testHierarchy2() {
        SpaceTypeDescriptor desc1 = new SpaceTypeDescriptorBuilder("d1", null).create();
        SpaceTypeDescriptor desc2 = new SpaceTypeDescriptorBuilder("d2", null).create();
        SpaceTypeDescriptor desc3 = new SpaceTypeDescriptorBuilder("d3", null).create();
        SpaceTypeDescriptor desc4 = new SpaceTypeDescriptorBuilder("d4", desc1).create();
        SpaceTypeDescriptor desc5 = new SpaceTypeDescriptorBuilder("d5", desc2).create();
        SpaceTypeDescriptor desc6 = new SpaceTypeDescriptorBuilder("d6", desc2).create();
        SpaceTypeDescriptor desc7 = new SpaceTypeDescriptorBuilder("d7", desc4).create();
        SpaceTypeDescriptor desc8 = new SpaceTypeDescriptorBuilder("d8", desc5).create();
        SpaceTypeDescriptor desc9 = new SpaceTypeDescriptorBuilder("d9", desc8).create();
        SpaceTypeDescriptor desc10 = new SpaceTypeDescriptorBuilder("d10", desc8).create();

        SpaceTypeDescriptorContainer container_desc1 = new SpaceTypeDescriptorContainer(desc1);
        SpaceTypeDescriptorContainer container_desc2 = new SpaceTypeDescriptorContainer(desc2);
        SpaceTypeDescriptorContainer container_desc3 = new SpaceTypeDescriptorContainer(desc3);
        SpaceTypeDescriptorContainer container_desc4 = new SpaceTypeDescriptorContainer(desc4);
        SpaceTypeDescriptorContainer container_desc5 = new SpaceTypeDescriptorContainer(desc5);
        SpaceTypeDescriptorContainer container_desc6 = new SpaceTypeDescriptorContainer(desc6);
        SpaceTypeDescriptorContainer container_desc7 = new SpaceTypeDescriptorContainer(desc7);
        SpaceTypeDescriptorContainer container_desc8 = new SpaceTypeDescriptorContainer(desc8);
        SpaceTypeDescriptorContainer container_desc9 = new SpaceTypeDescriptorContainer(desc9);
        SpaceTypeDescriptorContainer container_desc10 = new SpaceTypeDescriptorContainer(desc10);

        Map<String, SpaceTypeDescriptorContainer> typeDescriptorsData = new HashMap<String, SpaceTypeDescriptorContainer>();
        typeDescriptorsData.put(container_desc1.getTypeDescriptor().getTypeName(), container_desc1);
        typeDescriptorsData.put(container_desc2.getTypeDescriptor().getTypeName(), container_desc2);
        typeDescriptorsData.put(container_desc3.getTypeDescriptor().getTypeName(), container_desc3);
        typeDescriptorsData.put(container_desc4.getTypeDescriptor().getTypeName(), container_desc4);
        typeDescriptorsData.put(container_desc5.getTypeDescriptor().getTypeName(), container_desc5);
        typeDescriptorsData.put(container_desc6.getTypeDescriptor().getTypeName(), container_desc6);
        typeDescriptorsData.put(container_desc7.getTypeDescriptor().getTypeName(), container_desc7);
        typeDescriptorsData.put(container_desc8.getTypeDescriptor().getTypeName(), container_desc8);
        typeDescriptorsData.put(container_desc9.getTypeDescriptor().getTypeName(), container_desc9);
        typeDescriptorsData.put(container_desc10.getTypeDescriptor().getTypeName(), container_desc10);

        List<SpaceTypeDescriptor> sortedList = TypeDescriptorUtils.sort(typeDescriptorsData);

        assertOrder("d7", "d4", sortedList);
        assertOrder("d4", "d1", sortedList);
        assertOrder("d9", "d8", sortedList);
        assertOrder("d10", "d8", sortedList);
        assertOrder("d8", "d5", sortedList);
        assertOrder("d5", "d2", sortedList);
        assertOrder("d6", "d2", sortedList);
    }

    private void testHierarchy1() {
        SpaceTypeDescriptor aDesc = new SpaceTypeDescriptorBuilder(MyCassandraA.class, null).create();
        SpaceTypeDescriptor bDesc = new SpaceTypeDescriptorBuilder(MyCassandraB.class, aDesc).create();
        SpaceTypeDescriptor cDesc = new SpaceTypeDescriptorBuilder(MyCassandraC.class, null).create();

        SpaceTypeDescriptorContainer aDescContainer = new SpaceTypeDescriptorContainer(aDesc);
        SpaceTypeDescriptorContainer bDescContainer = new SpaceTypeDescriptorContainer(bDesc);
        SpaceTypeDescriptorContainer cDescContainer = new SpaceTypeDescriptorContainer(cDesc);

        Map<String, SpaceTypeDescriptorContainer> typeDescriptorsData = new HashMap<String, SpaceTypeDescriptorContainer>();
        typeDescriptorsData.put(aDescContainer.getTypeDescriptor().getTypeName(), aDescContainer);
        typeDescriptorsData.put(bDescContainer.getTypeDescriptor().getTypeName(), bDescContainer);
        typeDescriptorsData.put(cDescContainer.getTypeDescriptor().getTypeName(), cDescContainer);

        List<SpaceTypeDescriptor> sortedList = TypeDescriptorUtils.sort(typeDescriptorsData);

        assertOrder(MyCassandraB.class.getName(), MyCassandraA.class.getName(), sortedList);

    }

    private void assertOrder(String typeName, String superTypeName, List<SpaceTypeDescriptor> sortedList) {
        boolean seenSuper = false;
        for (SpaceTypeDescriptor spaceTypeDescriptor : sortedList) {
            if (superTypeName.equals(spaceTypeDescriptor.getTypeName()))
                seenSuper = true;
            else if (typeName.equals(spaceTypeDescriptor.getTypeName()) && !seenSuper)
                Assert.fail("Type " + superTypeName + " should come before " + typeName);
        }
    }

    public static class MyCassandraA {
        private String id1;

        @SpaceId
        public String getId1() {
            return id1;
        }

        public void setId1(String id) {
            this.id1 = id;
        }
    }

    public static class MyCassandraB extends MyCassandraA {
        private String id1;

        @SpaceId
        public String getId1() {
            return id1;
        }

        public void setId1(String id) {
            this.id1 = id;
        }
    }

    public static class MyCassandraC {
        private String id3;

        @SpaceId
        public String getId3() {
            return id3;
        }

        public void setId3(String id) {
            this.id3 = id;
        }
    }

}

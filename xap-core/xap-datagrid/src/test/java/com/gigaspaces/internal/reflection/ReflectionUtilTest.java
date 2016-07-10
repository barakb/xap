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

package com.gigaspaces.internal.reflection;

import com.gigaspaces.internal.reflection.standard.StandardField;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@com.gigaspaces.api.InternalApi
public class ReflectionUtilTest {
    public static class MyEntry {
        public String publicFieldA;
        public String publicFieldC;
        public String publicFieldB;

        public String privateFieldC;
        public String privateFieldA;
        public String privateFieldB;
    }

    public static class MySecondEntry extends MyEntry {
        public String publicFieldD;
        public String publicFieldE;
        public String publicFieldF;

        public String privateFieldF;
        public String privateFieldE;
        public String privateFieldD;
    }

    /**
     * Checks that ReflectionUtil.getFieldsForEntry returns the right fields for the given names.
     */
    @Test
    public void testGetCanonicalSortedFields() throws Exception {
        String[] names = new String[]{"privateFieldA", "privateFieldB", "privateFieldC",
                "publicFieldA", "publicFieldB", "publicFieldC",
                "privateFieldD", "privateFieldE", "privateFieldF",
                "publicFieldD", "publicFieldE", "publicFieldF"};

        List<IField> fields = ReflectionUtil.getCanonicalSortedFields(MySecondEntry.class);

        Assert.assertEquals("Number of fields", names.length, fields.size());
        for (int i = 0; i < names.length; ++i) {
            Assert.assertSame(StandardField.class, fields.get(i).getClass().getSuperclass());

            Assert.assertEquals("Field name", names[i], fields.get(i).getName());
        }
    }

    /**
     * Test static field access
     */
    @Test
    public void testInnerPrivateClassFieldAccess() throws Exception {
        List<IField> fields = ReflectionUtil.getCanonicalSortedFields(MyClassWithInner.class);
        for (IField f : fields) {
            Assert.assertSame(StandardField.class, f.getClass().getSuperclass());

            MyClassWithInner in = new MyClassWithInner();
            MyClassWithInner.TestNode node = new MyClassWithInner.TestNode();
            f.set(in, node);
            Assert.assertSame(node, f.get(in));
        }
    }

    public static class MyClassWithInner {
        public TestNode node;

        private static class TestNode {
            TestNode() {
            }
        }

        public MyClassWithInner() {
            node = new TestNode();
        }
    }

    @Test
    public void testGetAllInterfacesForClassAsSet() throws Exception {

        Set<Class> expected = new HashSet<Class>(Arrays.asList(new Class[]{A.class, B.class, C.class, D.class}));
        Set<Class> actual = ReflectionUtil.getAllInterfacesForClassAsSet(Sub.class);
        Assert.assertEquals(expected, actual);

        expected = new HashSet<Class>(Arrays.asList(new Class[]{B.class, C.class, D.class}));
        actual = ReflectionUtil.getAllInterfacesForClassAsSet(Super.class);
        Assert.assertEquals(expected, actual);

        expected = new HashSet<Class>(Arrays.asList(new Class[]{}));
        actual = ReflectionUtil.getAllInterfacesForClassAsSet(Empty.class);
        Assert.assertEquals(expected, actual);

        expected = new HashSet<Class>(Arrays.asList(new Class[]{B.class, C.class, D.class}));
        actual = ReflectionUtil.getAllInterfacesForClassAsSet(EmptySub.class);
        Assert.assertEquals(expected, actual);
    }

    public static class Empty {
    }

    public static class Sub extends Super implements A, C {
    }

    public static class EmptySub extends Super {
    }

    public static class Super implements B, D {
    }

    public interface A extends B, D {
    }

    public interface B extends C {
    }

    public interface C extends D {
    }

    public interface D {
    }
}

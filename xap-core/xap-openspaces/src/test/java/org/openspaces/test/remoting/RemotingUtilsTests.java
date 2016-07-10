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


package org.openspaces.test.remoting;

import com.gigaspaces.internal.reflection.IMethod;

import junit.framework.TestCase;

import org.openspaces.remoting.RemotingUtils;
import org.openspaces.remoting.RemotingUtils.MethodHash;

import java.lang.reflect.Method;
import java.util.Map;

/**
 * GS-8294: Methods of a Remoting Service interface are not included when it extends from another
 * interface
 *
 * @author moran
 */
public class RemotingUtilsTests extends TestCase {

    @Override
    protected void setUp() throws Exception {
    }

    public void testFastReflection() {
        fastReflection(true);
    }

    public void testStandardReflection() {
        fastReflection(false);
    }

    private void fastReflection(boolean fastReflection) {
        //+1 for added toString method (see case GS-8330)

        Map<MethodHash, IMethod> mapA = RemotingUtils.buildHashToMethodLookupForInterface(A.class, fastReflection);
        assertEquals(1 + 1, mapA.values().size());

        Map<MethodHash, IMethod> mapB = RemotingUtils.buildHashToMethodLookupForInterface(B.class, fastReflection);
        assertEquals(2 + 1, mapB.values().size());

        Map<MethodHash, IMethod> mapC = RemotingUtils.buildHashToMethodLookupForInterface(C.class, fastReflection);
        assertEquals(2 + 1, mapC.values().size());

        Map<MethodHash, IMethod> mapD = RemotingUtils.buildHashToMethodLookupForInterface(D.class, fastReflection);
        assertEquals(2 + 1, mapD.values().size());
    }

    public void testBuildMethodToHashLookupForInterface() {
        Map<Method, MethodHash> mapA = RemotingUtils.buildMethodToHashLookupForInterface(A.class, "asyncPrefix");
        assertEquals(1 + 1, mapA.values().size());

        Map<Method, MethodHash> mapB = RemotingUtils.buildMethodToHashLookupForInterface(B.class, "asyncPrefix");
        assertEquals(2 + 1, mapB.values().size());

        Map<Method, MethodHash> mapC = RemotingUtils.buildMethodToHashLookupForInterface(C.class, "asyncPrefix");
        assertEquals(2 + 1, mapC.values().size());

        Map<Method, MethodHash> mapD = RemotingUtils.buildMethodToHashLookupForInterface(C.class, "asyncPrefix");
        assertEquals(2 + 1, mapD.values().size());
    }

    public interface A {
        String a(String s);
    }

    public interface B extends A {
        String b(String s);
    }

    public interface C extends B {
    }

    public interface D extends C {
        @Override
        public String toString();
    }
}

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

/*
 * @(#)ClusterPolicyTest.java   Jan 1, 2008
 *
 * Copyright 2007 GigaSpaces Technologies Inc.
 */
package com.j_spaces.core.cluster;

import junit.framework.TestCase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * @author kimchy
 */
@com.gigaspaces.api.InternalApi
public class FailOverPolicyDescriptionTest extends TestCase {

    private FailOverPolicy.FailOverPolicyDescription full;
    private FailOverPolicy.FailOverPolicyDescription empty;
    private ExternalizableTestHelper helper;

    protected void setUp() throws Exception {
        helper = new ExternalizableTestHelper();
        full = new FailOverPolicy.FailOverPolicyDescription();
        full.m_PolicyType = 10;
        full.m_BackupOnly = new ArrayList<String>();
        full.m_BackupOnly.add("1");
        full.m_BackupOnly.add("2");
        full.m_BackupMemberNames = new HashMap<String, List<String>>();
        full.m_BackupMemberNames.put("a1", new ArrayList<String>());
        full.m_BackupMemberNames.get("a1").add("k1");
        empty = new FailOverPolicy.FailOverPolicyDescription();
    }

    /**
     * Test read and write empty ClusterPolicy object
     */
    public void testWriteReadEmpty() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream(0);
        ObjectOutputStream os = new ObjectOutputStream(out);
        os.writeObject(empty);
        ObjectInputStream is = new ObjectInputStream(new ByteArrayInputStream(out.toByteArray()));
        FailOverPolicy.FailOverPolicyDescription copy = (FailOverPolicy.FailOverPolicyDescription) is.readObject();
        is.close();
        os.close();
        assertNull(copy.m_BackupMemberNames);
        assertNull(copy.m_BackupOnly);
        assertEquals(-1, copy.m_PolicyType);
    }

    /**
     * Test read and write full ClusterPolicy object
     */
    public void testWriteReadFull() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream(0);
        ObjectOutputStream os = new ObjectOutputStream(out);
        os.writeObject(full);
        ObjectInputStream is = new ObjectInputStream(new ByteArrayInputStream(out.toByteArray()));
        FailOverPolicy.FailOverPolicyDescription copy = (FailOverPolicy.FailOverPolicyDescription) is.readObject();
        is.close();
        os.close();
        assertEquals("k1", copy.m_BackupMemberNames.get("a1").iterator().next());
    }
}
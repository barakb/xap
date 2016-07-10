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

import com.gigaspaces.cluster.replication.ReplicationTransmissionPolicy;

import junit.framework.TestCase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;


/**
 * @author kimchy
 */
@com.gigaspaces.api.InternalApi
public class ReplicationTransmissionPolicyTest extends TestCase {
    private ReplicationTransmissionPolicy replicationTransmissionPolicy;
    private ReplicationTransmissionPolicy empty;
    private ExternalizableTestHelper helper;

    protected void setUp() throws Exception {
        helper = new ExternalizableTestHelper();
        replicationTransmissionPolicy = helper.fill(new ReplicationTransmissionPolicy());
        empty = new ReplicationTransmissionPolicy();
    }

    /**
     * Test read and write empty ClusterPolicy object
     */
    public void testWriteReadEmpty() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream(0);
        ObjectOutputStream os = new ObjectOutputStream(out);
        os.writeObject(empty);
        ObjectInputStream is = new ObjectInputStream(new ByteArrayInputStream(out.toByteArray()));
        ReplicationTransmissionPolicy copy = (ReplicationTransmissionPolicy) is.readObject();
        is.close();
        os.close();
        assertTrue(helper.areEquals(empty, copy));
    }

    /**
     * Test read and write full ClusterPolicy object
     */
    public void testWriteReadFull() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream(0);
        ObjectOutputStream os = new ObjectOutputStream(out);
        os.writeObject(replicationTransmissionPolicy);
        ObjectInputStream is = new ObjectInputStream(new ByteArrayInputStream(out.toByteArray()));
        ReplicationTransmissionPolicy copy = (ReplicationTransmissionPolicy) is.readObject();
        is.close();
        os.close();
        assertTrue(helper.areEquals(replicationTransmissionPolicy, copy));
    }
}
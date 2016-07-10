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


package org.openspaces.test.core.cluster.info;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import static org.junit.Assert.assertNotNull;


/**
 * @author shaiw
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:/org/openspaces/test/core/cluster/info/cluster-info.xml")
public class ClusterInfoAnnotationsTest {
    @Autowired
    protected ApplicationContext ac;

    public ClusterInfoAnnotationsTest() {

    }

    protected String[] getConfigLocations() {
        return new String[]{"/org/openspaces/test/core/cluster/info/cluster-info.xml"};
    }

    @Test
    public void testClusterInfoAnnotations() {
        ClusterInfoBean clusterInfo = (ClusterInfoBean) ac.getBean("clusterInfoBean");
        assertNotNull(clusterInfo);
        assertNotNull(clusterInfo.info);
    }
}


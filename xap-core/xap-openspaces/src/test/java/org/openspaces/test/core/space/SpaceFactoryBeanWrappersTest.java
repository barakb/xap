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

package org.openspaces.test.core.space;

import com.j_spaces.core.IJSpace;

import junit.framework.TestCase;

import org.junit.Assert;
import org.junit.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * @author yuvalm
 */
public class SpaceFactoryBeanWrappersTest extends TestCase {

    public static final String SPACE_NAME = "myspace";
    public static final String EMBEDDED_SPACE_BEAN_ID = "emb";
    public static final String SPACE_PROXY_BEAN_ID = "prx";
    private ApplicationContext applicationContext;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        applicationContext = new ClassPathXmlApplicationContext("org/openspaces/test/core/space/url-space-wrappers-beans.xml");
    }

    @Test
    public void testSpaceWrappers() {
        IJSpace embeddedSpace = (IJSpace) applicationContext.getBean(EMBEDDED_SPACE_BEAN_ID);

        Assert.assertNotNull(embeddedSpace);
        String realUrl = embeddedSpace.getFinderURL().getURL();
        Assert.assertEquals(realUrl.substring(0, realUrl.indexOf('?')), "/./" + SPACE_NAME);

        IJSpace spaceProxy = (IJSpace) applicationContext.getBean(SPACE_PROXY_BEAN_ID);

        Assert.assertNotNull(spaceProxy);
        realUrl = spaceProxy.getFinderURL().getURL();
        Assert.assertEquals(realUrl.substring(0, realUrl.indexOf('?')), "jini://*/*/" + SPACE_NAME);
    }

}

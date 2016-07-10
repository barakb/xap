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

package com.j_spaces.jmx.util;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

/*******************************************************************************
 * Copyright (c) 2010 GigaSpaces Technologies Ltd. All rights reserved
 *
 * The software source code is proprietary and confidential information of GigaSpaces. You may use
 * the software source code solely under the terms and limitations of The license agreement granted
 * to you by GigaSpaces.
 *******************************************************************************/

@com.gigaspaces.api.InternalApi
public class ObjectNameFactory {
    public static final String TYPE_KEY = "type";
    public static final String NAME_KEY = "name";

    /**
     * Don't let anyone instantiate this class.
     */
    private ObjectNameFactory() {
    }

    /**
     * Build correct ObjectName for GigaSpaces JMX components.
     *
     * @return instance of <code>ObjectName</code>
     */
    public static ObjectName buildObjectName(String domain, String type, String name)
            throws MalformedObjectNameException, NullPointerException {
        return new ObjectName(domain + ':' + TYPE_KEY + '=' + type + ',' + NAME_KEY + '=' + name);
    }

}

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

package org.openspaces.memcached;

import org.openspaces.pu.service.PlainServiceDetails;

/**
 * @author kimchy (shay.banon)
 */
public class MemcachedServiceDetails extends PlainServiceDetails {

    private static final long serialVersionUID = 3543691389604775006L;

    public static final String SERVICE_TYPE = "memcached";

    public static class Attributes {
        public static final String SPACE = "space";
        public static final String PORT = "template";
    }

    public MemcachedServiceDetails() {
    }

    public MemcachedServiceDetails(String id, String space, int port) {
        super(id, "memcached", "memcached", "Memcached", "Memcached");
        getAttributes().put(Attributes.SPACE, space);
        getAttributes().put(Attributes.PORT, port);
    }

    public String getSpace() {
        return (String) getAttributes().get(Attributes.SPACE);
    }

    public Integer getPort() {
        return (Integer) getAttributes().get(Attributes.PORT);
    }
}

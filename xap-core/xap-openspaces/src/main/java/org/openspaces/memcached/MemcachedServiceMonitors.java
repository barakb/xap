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

import org.openspaces.pu.service.PlainServiceMonitors;

/**
 * @author kimchy (shay.banon)
 */
public class MemcachedServiceMonitors extends PlainServiceMonitors {

    private static final long serialVersionUID = -4220791859808730040L;

    public static class Attributes {
        public static final String GET_CMDS = "get-cmds";
        public static final String SET_CMDS = "set-cmds";
        public static final String GET_HITS = "get-hits";
        public static final String GET_MISSES = "get-misses";
    }

    public MemcachedServiceMonitors() {
        super();
    }

    public MemcachedServiceMonitors(String id, long getCmds, long setCmds, long getHits, long getMisses) {
        super(id);
        getMonitors().put(Attributes.GET_CMDS, getCmds);
        getMonitors().put(Attributes.SET_CMDS, setCmds);
        getMonitors().put(Attributes.GET_HITS, getHits);
        getMonitors().put(Attributes.GET_MISSES, getMisses);
    }

    public long getGetCmds() {
        return (Long) getMonitors().get(Attributes.GET_CMDS);
    }

    public long getSetCmds() {
        return (Long) getMonitors().get(Attributes.SET_CMDS);
    }

    public long getGetHits() {
        return (Long) getMonitors().get(Attributes.GET_HITS);
    }

    public long getGetMisses() {
        return (Long) getMonitors().get(Attributes.GET_MISSES);
    }
}

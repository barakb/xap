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

package org.openspaces.memcached.protocol;

import org.openspaces.memcached.LocalCacheElement;
import org.openspaces.memcached.SpaceCache;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;

/**
 * Represents the response to a command.
 */
public final class ResponseMessage implements Serializable {

    private static final long serialVersionUID = -363616355081114688L;

    public ResponseMessage(CommandMessage cmd) {
        this.cmd = cmd;
    }

    public CommandMessage cmd;
    public LocalCacheElement[] elements;
    public SpaceCache.StoreResponse response;
    public Map<String, Set<String>> stats;
    public String version;
    public SpaceCache.DeleteResponse deleteResponse;
    public Integer incrDecrResponse;
    public boolean flushSuccess;

    public ResponseMessage withElements(LocalCacheElement[] elements) {
        this.elements = elements;
        return this;
    }

    public ResponseMessage withResponse(SpaceCache.StoreResponse response) {
        this.response = response;
        return this;
    }

    public ResponseMessage withDeleteResponse(SpaceCache.DeleteResponse deleteResponse) {
        this.deleteResponse = deleteResponse;
        return this;
    }

    public ResponseMessage withIncrDecrResponse(Integer incrDecrResp) {
        this.incrDecrResponse = incrDecrResp;

        return this;
    }

    public ResponseMessage withStatResponse(Map<String, Set<String>> stats) {
        this.stats = stats;

        return this;
    }

    public ResponseMessage withFlushResponse(boolean success) {
        this.flushSuccess = success;

        return this;
    }
}

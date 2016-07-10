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

package org.openspaces.test.pojos;

import com.gigaspaces.annotation.pojo.SpaceId;
import com.gigaspaces.annotation.pojo.SpaceRouting;

/**
 * POJO mock for using with Container templates
 *
 * @author Idan Moyal
 * @since 8.0.1
 */
public class MockPojo {

    private String id;
    private Boolean processed;
    private Integer routing;

    public MockPojo() {
    }

    public MockPojo(Boolean processed, Integer routing) {
        this.processed = processed;
        this.setRouting(routing);
    }

    @SpaceId(autoGenerate = true)
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Boolean getProcessed() {
        return processed;
    }

    public void setProcessed(Boolean processed) {
        this.processed = processed;
    }

    @Override
    public String toString() {
        return "MockPojo [" + (id != null ? "id=" + id + ", " : "")
                + (processed != null ? "processed=" + processed + ", " : "")
                + (routing != null ? "routing=" + routing : "") + "]";
    }

    public void setRouting(Integer routing) {
        this.routing = routing;
    }

    @SpaceRouting
    public Integer getRouting() {
        return routing;
    }

}

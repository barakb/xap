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


package org.openspaces.admin.quiesce;

import com.gigaspaces.admin.quiesce.InstancesQuiesceState;
import com.gigaspaces.admin.quiesce.QuiesceState;


/**
 * Represents the quiesce details of the processing unit. If a quiesce request failed for some
 * reason ({@link org.openspaces.admin.pu.ProcessingUnit#waitFor(com.gigaspaces.admin.quiesce.QuiesceState)}
 * returned false after requesting quiesce/unquiesce) It's possible to investigate the failure
 * reason by inspecting {@link InstancesQuiesceState}.
 *
 * @author Boris
 * @since 10.1.0
 */
public class QuiesceDetails {

    private QuiesceState status;
    private String description;
    private InstancesQuiesceState instancesQuiesceState;

    public QuiesceDetails(QuiesceState status, String description, InstancesQuiesceState instancesQuiesceState) {
        this.status = status;
        this.description = description;
        this.instancesQuiesceState = instancesQuiesceState;
    }

    public QuiesceState getStatus() {
        return status;
    }

    public String getDescription() {
        return description;
    }

    public InstancesQuiesceState getInstancesQuiesceState() {
        return instancesQuiesceState;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        QuiesceDetails that = (QuiesceDetails) o;

        if (description != null ? !description.equals(that.description) : that.description != null)
            return false;
        if (status != that.status) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = status != null ? status.hashCode() : 0;
        result = 31 * result + (description != null ? description.hashCode() : 0);
        return result;
    }
}

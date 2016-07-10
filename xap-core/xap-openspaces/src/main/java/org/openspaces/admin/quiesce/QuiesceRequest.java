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

/**
 * A request which includes all needed information about the quiesce request (e.g reason for
 * entering to QUIESCE mode , the user name which triggered the operation, the time and date and so
 * on)
 *
 * @author Boris
 * @since 10.1.0
 */
public class QuiesceRequest {

    private String description;

    public QuiesceRequest(String description) {
        this.description = description;
    }

    public String getDescription() {
        return description;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        QuiesceRequest that = (QuiesceRequest) o;

        if (description != null ? !description.equals(that.description) : that.description != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        return description != null ? description.hashCode() : 0;
    }

    @Override
    public String toString() {
        return "QuiesceRequest{" +
                "description='" + description + '\'' +
                '}';
    }
}

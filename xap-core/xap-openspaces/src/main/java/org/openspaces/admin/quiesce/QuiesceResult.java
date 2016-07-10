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

import com.gigaspaces.admin.quiesce.QuiesceToken;


/**
 * A result of triggering {@link org.openspaces.admin.pu.ProcessingUnit#quiesce(QuiesceRequest)} or
 * {@link org.openspaces.admin.pu.ProcessingUnit#unquiesce(QuiesceRequest)} (QuiesceRequest)} The
 * result contains {@link com.gigaspaces.admin.quiesce.QuiesceToken} and description about the
 * request.
 *
 * @author Boris
 * @since 10.1.0
 */
public class QuiesceResult {

    private QuiesceToken token;
    private String description;

    public QuiesceResult(QuiesceToken token, String description) {
        this.token = token;
        this.description = description;
    }

    public QuiesceToken getToken() {
        return token;
    }

    public String getDescription() {
        return description;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        QuiesceResult that = (QuiesceResult) o;

        return token.equals(that.token);

    }

    @Override
    public int hashCode() {
        return token.hashCode();
    }

    @Override
    public String toString() {
        return "QuiesceResult{" +
                "token=" + token +
                ", description='" + description + '\'' +
                '}';
    }
}

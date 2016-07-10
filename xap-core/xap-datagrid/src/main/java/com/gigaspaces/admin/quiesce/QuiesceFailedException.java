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


package com.gigaspaces.admin.quiesce;

/**
 * This exception is thrown when the quiesce request is rejected by the {@link
 * com.gigaspaces.grid.gsm.GSM}
 *
 * @author Boris
 * @since 10.1.0
 */
@com.gigaspaces.api.InternalApi
public class QuiesceFailedException extends Exception {

    private static final long serialVersionUID = -255555510996475145L;

    private QuiesceState status = null;
    private String description = null;

    public QuiesceFailedException(String message, QuiesceState status, String description) {
        super(message + ", Description: " + ((description == null || description.isEmpty()) ? "(empty)" : description));
        this.status = status;
        this.description = description;
    }

    public QuiesceFailedException(String message) {
        super(message);
    }

    public QuiesceState getStatus() {
        return status;
    }

    public String getDescription() {
        return description;
    }
}

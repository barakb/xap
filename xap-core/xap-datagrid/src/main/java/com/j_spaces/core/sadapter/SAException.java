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

package com.j_spaces.core.sadapter;

/**
 * SAException is a class that wraps exception that are thrown at the storage adapter. This
 * exception is unique to the storage adapter.
 **/
@com.gigaspaces.api.InternalApi
public class SAException extends Exception {
    private static final long serialVersionUID = -5947884915696434520L;

    public SAException() {
        super();
    }

    public SAException(String msg) {
        super(msg);
    }

    public SAException(Throwable cause) {
        super(cause);
    }

    public SAException(String msg, Throwable cause) {
        super(msg, cause);
    }
}

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

package com.gigaspaces.exception.lrmi;

/**
 * Created by Barak Bar Orion 6/24/15.
 */
@com.gigaspaces.api.InternalApi
public class RMIShutDownException extends ProxyClosedException {
    private static final long serialVersionUID = 1;

    public RMIShutDownException() {
    }

    public RMIShutDownException(String s) {
        super(s);
    }

    public RMIShutDownException(String s, Throwable cause) {
        super(s, cause);
    }
}

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

package com.j_spaces.core.exception.internal;


/**
 * A wrapper class for the {@link InterruptedException}. Used to throw {@link InterruptedException}
 * even if the API method signature doesn't have InterruptedException.
 *
 * @author anna
 * @since 6.5
 */
@com.gigaspaces.api.InternalApi
public class InterruptedSpaceException extends ProxyInternalSpaceException {
    private static final long serialVersionUID = 1L;

    public InterruptedSpaceException(InterruptedException cause) {
        super(cause.toString(), cause);
    }

    public InterruptedSpaceException(String message, InterruptedException cause) {
        super(message, cause);
    }
}

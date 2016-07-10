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


package org.openspaces.core.space.filter;

import org.springframework.core.NestedRuntimeException;

/**
 * A nested runtime exception wrapping a filter execution exception.
 *
 * @author kimchy
 */
public class FilterExecutionException extends NestedRuntimeException {

    private static final long serialVersionUID = -1291118446669719067L;

    public FilterExecutionException(String msg) {
        super(msg);
    }

    public FilterExecutionException(String msg, Throwable cause) {
        super(msg, cause);
    }
}

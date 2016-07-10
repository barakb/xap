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

package com.gigaspaces.internal.dump;

/**
 * @author kimchy
 */
@com.gigaspaces.api.InternalApi
public class InternalDumpProcessorFailedException extends InternalDumpException {
    private static final long serialVersionUID = -5268461169409900125L;

    private final String name;

    public InternalDumpProcessorFailedException(String name, String msg) {
        this(name, msg, null);
    }

    public InternalDumpProcessorFailedException(String name, String msg, Throwable cause) {
        super(name + ": " + msg, cause);
        this.name = name;
    }

    public String name() {
        return this.name;
    }
}

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


package com.gigaspaces.cluster.replication.gateway.conflict;

/**
 * A {@link ConflictCause} implementation for a general conflict.
 *
 * @author eitany
 * @since 9.5
 */

public class GeneralConflictCause
        extends ConflictCause {

    private final Throwable _error;

    public GeneralConflictCause(Throwable error) {
        _error = error;
    }

    @Override
    public String toString() {
        return "general conflict - " + _error.getMessage();
    }

    /**
     * Returns the error that causes this conflict.
     */
    public Throwable getError() {
        return _error;
    }

}

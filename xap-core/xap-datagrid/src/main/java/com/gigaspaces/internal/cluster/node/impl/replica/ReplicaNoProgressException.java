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

package com.gigaspaces.internal.cluster.node.impl.replica;

/**
 * Thrown when replica process has no progress for predefined timeout configuration and therefore
 * the replica is aborted and failed.
 *
 * @author eitany
 * @since 9.1
 */
@com.gigaspaces.api.InternalApi
public class ReplicaNoProgressException
        extends Exception {

    private static final long serialVersionUID = 1L;

    public ReplicaNoProgressException(String message) {
        super(message);
    }

}

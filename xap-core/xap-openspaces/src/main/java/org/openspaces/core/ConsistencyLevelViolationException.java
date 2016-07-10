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

package org.openspaces.core;

import com.gigaspaces.cluster.replication.ConsistencyLevel;

import org.springframework.dao.DataAccessException;

/**
 * Thrown when an operation is rejected since the {@link ConsistencyLevel} for that operation cannot
 * be maintained. {@link com.gigaspaces.cluster.replication.ConsistencyLevelViolationException}
 *
 * @author eitany
 * @since 9.5.1
 */
public class ConsistencyLevelViolationException
        extends DataAccessException {
    private static final long serialVersionUID = 1L;

    public ConsistencyLevelViolationException(com.gigaspaces.cluster.replication.ConsistencyLevelViolationException e) {
        super(e.getMessage(), e);
    }


}

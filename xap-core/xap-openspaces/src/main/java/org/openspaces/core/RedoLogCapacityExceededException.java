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

/**
 * This exeception indicates the redo log reached its planned capacity and operations should be
 * blocked until the redolog size is reduced {@link com.gigaspaces.cluster.replication.RedoLogCapacityExceededException}
 *
 * @author eitany
 * @since 7.1
 */
public class RedoLogCapacityExceededException extends ResourceCapacityExceededException {

    private static final long serialVersionUID = 234800445050248452L;

    private final com.gigaspaces.cluster.replication.RedoLogCapacityExceededException e;

    public RedoLogCapacityExceededException(com.gigaspaces.cluster.replication.RedoLogCapacityExceededException e) {
        super(e.getMessage(), e);
        this.e = e;
    }

    /**
     * @return name of the target that the redo log is kept for
     */
    public String getTargetName() {
        return e.getTargetName();
    }

    /**
     * @return name of the space that is keeping the redo log
     */
    public String getSpaceName() {
        return e.getSpaceName();
    }


}

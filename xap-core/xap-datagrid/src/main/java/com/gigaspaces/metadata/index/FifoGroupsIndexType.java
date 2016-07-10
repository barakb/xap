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


package com.gigaspaces.metadata.index;

/**
 * Determines a Space index type For Fifo-groups usage.
 *
 * @author Yechiel
 * @since 9.0
 */

public enum FifoGroupsIndexType {
    /**
     * Not used .
     */
    NONE,
    /**
     * main fifo groups index index - grouping done by its value.
     */
    MAIN,
    /**
     * auxiliary index -used in fifo groups selection templates, compound index is created together
     * with the fifo groups main index.
     */
    AUXILIARY;


}

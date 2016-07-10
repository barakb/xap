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

package com.j_spaces.jdbc.batching;

import com.j_spaces.jdbc.ResponsePacket;

/**
 * A packet returned from a JDBC batch execution.
 *
 * The result held in the packet is an array of Integers. Each Integer represents a result returned
 * from a single query execution from the batch.
 *
 * The value indicates how many rows were affected by the execution.
 *
 * @author idan
 * @since 8.0
 */
@com.gigaspaces.api.InternalApi
public class BatchResponsePacket extends ResponsePacket {

    private int[] _result;
    //
    private static final long serialVersionUID = 1L;

    /**
     * Constructs a new batch response packet using the provided ints array.
     */
    public BatchResponsePacket(int[] result) {
        _result = result;
    }

    /**
     * Gets the batch response packet's result
     */
    public int[] getResult() {
        return _result;
    }

}

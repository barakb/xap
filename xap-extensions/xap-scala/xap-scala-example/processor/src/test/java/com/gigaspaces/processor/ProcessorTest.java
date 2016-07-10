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

package com.gigaspaces.processor;

import com.gigaspaces.common.Data;

import org.junit.Test;

import static org.junit.Assert.assertEquals;


/**
 * A simple unit test that verifies the Processor processData method actually processes the Data
 * object.
 */
public class ProcessorTest {

    @Test
    public void verifyProcessedFlag() {
        Processor processor = new Processor();
        Data data = new Data(1, "test");

        Data result = processor.processData(data);
        assertEquals("verify that the data object was processed", true, result.isProcessed());
        assertEquals("verify the data was processed", "PROCESSED : " + data.getRawData(), result.getData());
        assertEquals("verify the type was not changed", data.getType(), result.getType());
    }
}

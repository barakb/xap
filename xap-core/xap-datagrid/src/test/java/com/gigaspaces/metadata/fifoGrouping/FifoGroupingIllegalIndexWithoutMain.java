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

package com.gigaspaces.metadata.fifoGrouping;

import com.gigaspaces.annotation.pojo.SpaceFifoGroupingIndex;
import com.gigaspaces.annotation.pojo.SpaceId;

@com.gigaspaces.api.InternalApi
public class FifoGroupingIllegalIndexWithoutMain {
    private Integer id;
    private String symbol;

    public FifoGroupingIllegalIndexWithoutMain() {
    }

    public FifoGroupingIllegalIndexWithoutMain(Integer id, String symbol) {
        this.id = id;
        this.symbol = symbol;
    }

    @SpaceId
    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    @SpaceFifoGroupingIndex
    public String getSymbol() {
        return symbol;
    }

    public void setSymbol(String symbol) {
        this.symbol = symbol;
    }

}

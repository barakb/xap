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

import com.gigaspaces.annotation.pojo.SpaceClass;
import com.gigaspaces.annotation.pojo.SpaceFifoGroupingProperty;
import com.gigaspaces.annotation.pojo.SpaceId;
import com.gigaspaces.annotation.pojo.SpaceVersion;

import java.sql.Timestamp;

@SpaceClass
@com.gigaspaces.api.InternalApi
public class FifoGroupsDoubleDefExceptionData {
    private Integer id;
    private String symbol;
    private Integer version;
    private String reporter;
    private Timestamp timeStamp;
    private boolean processed;
    private Integer state = 0;

    public FifoGroupsDoubleDefExceptionData(Integer id, String symbol, String reporter) {
        super();
        this.id = id;
        this.symbol = symbol;
        this.setReporter(reporter);
    }

    public FifoGroupsDoubleDefExceptionData(Integer id, String symbol) {
        super();
        this.id = id;
        this.symbol = symbol;
    }

    public FifoGroupsDoubleDefExceptionData() {
        super();
    }

    @SpaceId
    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    @SpaceFifoGroupingProperty()
    public String getSymbol() {
        return symbol;
    }

    public void setSymbol(String symbol) {
        this.symbol = symbol;
    }

    @SpaceVersion
    public Integer getVersion() {
        return version;
    }

    public void setVersion(Integer version) {
        this.version = version;
    }

    public void setReporter(String reporter) {
        this.reporter = reporter;
    }

    @SpaceFifoGroupingProperty()
    public String getReporter() {
        return reporter;
    }

    public void setTimeStamp(Timestamp timeStamp) {
        this.timeStamp = timeStamp;
    }

    public Timestamp getTimeStamp() {
        return timeStamp;
    }

    public void setProcessed(boolean processed) {
        this.processed = processed;
    }

    public boolean isProcessed() {
        return processed;
    }

    public void setState(Integer state) {
        this.state = state;
    }

    public Integer getState() {
        return state;
    }

}

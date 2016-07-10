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
import com.gigaspaces.annotation.pojo.SpaceFifoGroupingProperty;
import com.gigaspaces.annotation.pojo.SpaceId;

import java.sql.Timestamp;
import java.util.LinkedList;
import java.util.List;

@com.gigaspaces.api.InternalApi
public class FifoGroupingWithPathPojo {
    private Integer id;
    private Info info;
    private Timestamp time;

    public FifoGroupingWithPathPojo() {
    }

    public FifoGroupingWithPathPojo(Integer id, String reporter, long time) {
        super();
        this.id = id;
        this.info = new Info(reporter, null);
        this.time = new Timestamp(time);
    }

    @SpaceId
    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    @SpaceFifoGroupingProperty(path = "reporter")
    public Info getInfo() {
        return info;
    }

    public void setInfo(Info info) {
        this.info = info;
    }

    @SpaceFifoGroupingIndex
    public Timestamp getTime() {
        return time;
    }

    public void setTime(Timestamp time) {
        this.time = time;
    }

    public class Info {
        String reporter;
        List<String> lastReports;

        Info() {
        }

        Info(String reporter, List<String> lastReports) {
            this.reporter = reporter;
            if (lastReports == null)
                this.lastReports = new LinkedList<String>();
            else
                this.lastReports = lastReports;
        }

        public String getReporter() {
            return reporter;
        }

        public void setReporter(String reporter) {
            this.reporter = reporter;
        }

        public List<String> getLastReports() {
            return lastReports;
        }

        public void setLastReports(List<String> lastReports) {
            this.lastReports = lastReports;
        }
    }
}

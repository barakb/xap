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


package com.gigaspaces.query.aggregators;

/**
 * @author Niv Ingberg
 * @since 10.0
 */

public class CountAggregator extends AbstractPathAggregator<Long> {

    private static final long serialVersionUID = 1L;

    private transient long result;

    @Override
    public String getDefaultAlias() {
        String path = getPath();
        return path == null ? "count(*)" : "count(" + path + ")";
    }

    @Override
    public AbstractPathAggregator setPath(String path) {
        if (path == null || path.length() == 0 || path.equals("*"))
            path = null;
        return super.setPath(path);
    }

    @Override
    public void aggregate(SpaceEntriesAggregatorContext context) {
        if (getPath() == null || getPathValue(context) != null)
            result++;
    }

    @Override
    public void aggregateIntermediateResult(Long partitionResult) {
        this.result += partitionResult;
    }

    @Override
    public Long getIntermediateResult() {
        return result;
    }
}

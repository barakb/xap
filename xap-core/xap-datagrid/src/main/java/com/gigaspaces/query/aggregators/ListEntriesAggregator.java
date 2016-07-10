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

import com.gigaspaces.internal.query.RawEntry;

import java.util.ArrayList;

/**
 * @author Niv Ingberg
 * @since 10.0
 */

public class ListEntriesAggregator extends SpaceEntriesAggregator<ArrayList<RawEntry>> {

    private static final long serialVersionUID = 1L;

    private transient ArrayList<RawEntry> result;

    @Override
    public String getDefaultAlias() {
        return "ListEntries";
    }

    @Override
    public void aggregate(SpaceEntriesAggregatorContext context) {
        if (result == null)
            result = new ArrayList<RawEntry>();
        result.add(context.getRawEntry());
    }

    @Override
    public ArrayList<RawEntry> getIntermediateResult() {
        return result;
    }

    @Override
    public void aggregateIntermediateResult(ArrayList<RawEntry> partitionResult) {
        if (partitionResult != null) {
            if (result == null)
                result = partitionResult;
            else
                result.addAll(partitionResult);
        }
    }

    @Override
    public Object getFinalResult() {
        // HACK: convert within the same list to conserve memory allocation.
        ArrayList list = result;
        if (list != null) {
            for (int i = 0; i < list.size(); i++)
                list.set(i, toObject(result.get(i)));
        }
        return result;
    }
}

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

import com.gigaspaces.internal.utils.math.MutableNumber;

/**
 * @author Niv Ingberg
 * @since 10.0
 */

public class SumAggregator extends AbstractPathAggregator<MutableNumber> {

    private static final long serialVersionUID = 1L;

    private transient MutableNumber result;

    @Override
    public String getDefaultAlias() {
        return "sum(" + getPath() + ")";
    }

    @Override
    public void aggregate(SpaceEntriesAggregatorContext context) {
        add((Number) getPathValue(context));
    }

    @Override
    public MutableNumber getIntermediateResult() {
        return result;
    }

    @Override
    public void aggregateIntermediateResult(MutableNumber partitionResult) {
        if (result == null)
            result = partitionResult;
        else
            result.add(partitionResult.toNumber());
    }

    @Override
    public Object getFinalResult() {
        return result != null ? result.toNumber() : null;
    }

    private void add(Number number) {
        if (number != null) {
            if (result == null)
                result = MutableNumber.fromClass(number.getClass(), true);
            result.add(number);
        }
    }
}

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
import com.gigaspaces.internal.query.RawEntryConverter;

import java.io.Serializable;

/**
 * @author Niv Ingberg
 * @since 10.0
 */
public abstract class SpaceEntriesAggregator<T extends Serializable> implements Serializable, Cloneable {

    private static final long serialVersionUID = 1L;

    private transient String alias;
    private transient RawEntryConverter rawEntryConverter;

    public String getAlias() {
        return alias;
    }

    public SpaceEntriesAggregator as(String alias) {
        this.alias = alias;
        return this;
    }

    public abstract String getDefaultAlias();

    public abstract void aggregate(SpaceEntriesAggregatorContext context);

    public abstract T getIntermediateResult();

    public abstract void aggregateIntermediateResult(T partitionResult);

    public Object getFinalResult() {
        return getIntermediateResult();
    }

    protected void setRawEntryConverter(RawEntryConverter rawEntryConverter) {
        this.rawEntryConverter = rawEntryConverter;
    }

    protected Object toObject(RawEntry rawEntry) {
        return rawEntryConverter.toObject(rawEntry);
    }

    @Override
    public SpaceEntriesAggregator<T> clone() {
        try {
            return (SpaceEntriesAggregator<T>) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new IllegalStateException("Failed to clone a cloneable object", e);
        }
    }
}

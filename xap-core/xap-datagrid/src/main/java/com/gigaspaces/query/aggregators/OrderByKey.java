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

import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.query.CompoundResult;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author anna
 * @since 10.1
 */

public class OrderByKey extends CompoundResult implements Comparable<OrderByKey> {

    private static final long serialVersionUID = 1L;
    private List<OrderByPath> orderByPaths;

    /**
     * Required for Externalizable
     */
    public OrderByKey() {
    }

    public OrderByKey(Object[] values) {
        super(values, null);
    }

    protected OrderByKey(int numOfValues) {
        super(new Object[numOfValues], null);
    }

    protected boolean initialize(List<OrderByPath> orderByPaths, SpaceEntriesAggregatorContext context) {
        this.orderByPaths = orderByPaths;
        this.nameIndexMap = new HashMap<String, Integer>();

        hashCode = 0;
        for (int i = 0; i < orderByPaths.size(); i++) {
            values[i] = context.getPathValue(orderByPaths.get(i).getPath());
            nameIndexMap.put(orderByPaths.get(i).getPath(), i);
        }

        return true;
    }

    protected void setNameIndex(Map<String, Integer> nameIndexMap) {
        this.nameIndexMap = nameIndexMap;
    }

    @Override
    public int compareTo(OrderByKey orderKey) {
        int rc = 0;

        for (int i = 0; i < orderByPaths.size(); i++) {
            OrderByPath orderByPath = orderByPaths.get(i);

            Comparable c1 = (Comparable) get(orderByPath.getPath());
            Comparable c2 = (Comparable) orderKey.get(orderByPath.getPath());

            if (c1 == c2)
                continue;

            if (c1 == null)
                return orderByPath.isNullsLast() ? 1 : -1;

            if (c2 == null)
                return orderByPath.isNullsLast() ? -1 : 1;

            rc = c1.compareTo(c2);
            if (rc != 0)
                return orderByPath.getOrderBy() == OrderBy.DESC ? -rc : rc;

        }

        return rc;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        IOUtils.writeObject(out, orderByPaths);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        this.orderByPaths = IOUtils.readObject(in);
    }
}

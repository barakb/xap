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

package com.gigaspaces.internal.collections.standard;

import com.gigaspaces.internal.collections.IntegerList;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Niv Ingberg
 * @since 12.0
 */
@com.gigaspaces.api.InternalApi
public class StandardIntegerList implements IntegerList {
    private final List<Integer> list = new ArrayList<Integer>();

    @Override
    public boolean isEmpty() {
        return list.isEmpty();
    }

    @Override
    public int size() {
        return list.size();
    }

    @Override
    public int getQuick(int pos) {
        return list.get(pos);
    }

    @Override
    public void add(int val) {
        list.add(val);
    }

    @Override
    public int[] toNativeArray() {
        int[] result = new int[list.size()];
        for (int i = 0; i < result.length; i++)
            result[i] = list.get(i);
        return result;
    }
}

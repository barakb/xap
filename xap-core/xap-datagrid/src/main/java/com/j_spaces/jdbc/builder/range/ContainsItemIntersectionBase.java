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

package com.j_spaces.jdbc.builder.range;

import com.gigaspaces.internal.query.IContainsItemsCustomQuery;


/**
 * info for contains items intersection. currently  only segment-ranges are supported
 *
 * @author Yechiel Fefer
 * @since 9.6
 */
@com.gigaspaces.api.InternalApi
public class ContainsItemIntersectionBase {

    private final String _path;
    private IContainsItemsCustomQuery _root;

    public ContainsItemIntersectionBase(ContainsItemValueRange baseRange) {
        _path = baseRange.getPath();
        _root = baseRange.getRoot();
    }


    @Override
    public boolean equals(Object other) {
        if (other instanceof ContainsItemIntersectionBase) {
            ContainsItemIntersectionBase o = (ContainsItemIntersectionBase) other;
            return _root == o._root && _path.equals(o._path);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return _path.hashCode() | _root.hashCode();
    }
}

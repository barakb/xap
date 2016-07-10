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


package com.j_spaces.core.cache;

import com.j_spaces.kernel.IObjectInfo;

import java.util.ArrayList;

/**
 * TODO	add Javadoc
 *
 * @author Yechiel Fefer
 * @version 1.0
 * @since 8.0
 */
/*
 * a single backref used for multi value collection
 */

@com.gigaspaces.api.InternalApi
public class MultiValueIndexBackref
        implements IObjectInfo<IEntryCacheInfo> {
    static final long serialVersionUID = 1L;


    private final transient ArrayList<IObjectInfo<IEntryCacheInfo>> _backRefs;

    public MultiValueIndexBackref(int size) {
        _backRefs = new ArrayList<IObjectInfo<IEntryCacheInfo>>(size);
    }

    public ArrayList<IObjectInfo<IEntryCacheInfo>> getBackrefs() {
        return _backRefs;
    }


    //dummy methods for interface
    public void setSubject(IEntryCacheInfo subject) {
        throw new UnsupportedOperationException();
    }

    public IEntryCacheInfo getSubject() {
        throw new UnsupportedOperationException();
    }

}

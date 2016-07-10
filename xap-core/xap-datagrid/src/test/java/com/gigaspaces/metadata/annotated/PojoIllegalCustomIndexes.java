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

package com.gigaspaces.metadata.annotated;

import com.gigaspaces.internal.metadata.annotations.CustomSpaceIndex;
import com.gigaspaces.internal.metadata.annotations.CustomSpaceIndexes;
import com.gigaspaces.internal.query.valuegetter.ISpaceValueGetter;
import com.gigaspaces.internal.query.valuegetter.SpaceEntryPropertyGetter;
import com.gigaspaces.server.ServerEntry;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Duplicate index definition
 *
 * @author anna
 * @since 7.1
 */
@CustomSpaceIndexes(value = {
        @CustomSpaceIndex(name = "compoundIndex", indexValueGetter = PojoIllegalCustomIndexes.MyValueGetter.class),
        @CustomSpaceIndex(name = "compoundIndex", indexValueGetter = PojoIllegalCustomIndexes.MyValueGetter.class)})
@com.gigaspaces.api.InternalApi
public class PojoIllegalCustomIndexes {
    /**
     * @author anna
     * @since 7.1
     */
    public static class MyValueGetter implements ISpaceValueGetter<ServerEntry> {

        private SpaceEntryPropertyGetter str1Getter = new SpaceEntryPropertyGetter("str1");
        private SpaceEntryPropertyGetter str2Getter = new SpaceEntryPropertyGetter("str2");

        /*
         * (non-Javadoc)
         * 
         * @see com.gigaspaces.query.valuegetter.AbstractSpaceValueGetter#getValue(java.lang.Object)
         */
        public Object getValue(ServerEntry target) {
            return str1Getter.getValue(target) + ":" + str2Getter.getValue(target);
        }

        /* (non-Javadoc)
         * @see java.io.Externalizable#readExternal(java.io.ObjectInput)
         */
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            // TODO Auto-generated method stub

        }

        /* (non-Javadoc)
         * @see java.io.Externalizable#writeExternal(java.io.ObjectOutput)
         */
        public void writeExternal(ObjectOutput out) throws IOException {
            // TODO Auto-generated method stub

        }

        @Override
        public void writeToSwap(ObjectOutput out) throws IOException {
            // TODO Auto-generated method stub

        }

        @Override
        public void readFromSwap(ObjectInput in) throws IOException, ClassNotFoundException {
            // TODO Auto-generated method stub

        }

    }

    private String _str1;
    private String _str2;

    public String getStr1() {
        return _str1;
    }

    public void setStr1(String str1) {
        _str1 = str1;
    }

    public String getStr2() {
        return _str2;
    }

    public void setStr2(String str2) {
        _str2 = str2;
    }


}

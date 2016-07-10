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

package org.openspaces.itest.persistency.cassandra.spring;

import org.openspaces.persistency.cassandra.meta.types.SerializerProvider;
import org.openspaces.persistency.cassandra.meta.types.dynamic.PropertyValueSerializer;

import java.nio.ByteBuffer;

/**
 * @author dank
 */
public class MockPropertySerializer implements PropertyValueSerializer {

    @Override
    public ByteBuffer toByteBuffer(Object value) {
        return SerializerProvider.getObjectSerializer().toByteBuffer(value);
    }

    @Override
    public Object fromByteBuffer(ByteBuffer byteBuffer) {
        return SerializerProvider.getObjectSerializer().fromByteBuffer(byteBuffer);
    }

}

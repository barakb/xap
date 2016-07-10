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

package org.openspaces.persistency.cassandra.meta.types;

import java.nio.ByteBuffer;

import me.prettyprint.cassandra.serializers.AbstractSerializer;
import me.prettyprint.hector.api.Serializer;

/**
 * A {@link Serializer} implementation for {@link Byte} values.
 *
 * @author Dan Kilman
 * @since 9.1.1
 */
public class ByteSerializer
        extends AbstractSerializer<Byte>
        implements Serializer<Byte> {

    private static final ByteSerializer INSTANCE = new ByteSerializer();

    public static Serializer<Byte> get() {
        return INSTANCE;
    }

    @Override
    public ByteBuffer toByteBuffer(Byte obj) {
        return ByteBuffer.wrap(new byte[]{obj});
    }

    @Override
    public Byte fromByteBuffer(ByteBuffer byteBuffer) {
        return byteBuffer.get();
    }

}

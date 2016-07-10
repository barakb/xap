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

package org.openspaces.utest.persistency.cassandra.meta.types;

import org.junit.Assert;
import org.junit.Test;
import org.openspaces.persistency.cassandra.meta.types.dynamic.DynamicPropertyValueSerializer;
import org.openspaces.test.common.data.TestPojo1;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;
import java.util.UUID;


public class DynamicPropertyByteBufferConverterTest {

    @Test
    public void test() {
        test('a');
        test((byte) 23);

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 0xFFFFL * 2; i++)
            sb.append('a');
        test(sb.toString());

        test(true);
        test(1234);
        test(31434324324l);
        test(12.12f);
        test(123.123);
        test((short) 12323);
        test(UUID.randomUUID());
        test(new Date(123456789));
        test(new TestPojo1("some string"));
        test(new BigInteger("123123"));
        test(new BigDecimal(new BigInteger("123213213")));
        test(new byte[]{1, 2, 3, 4});

    }

    private static void test(Object value) {
        DynamicPropertyValueSerializer converter = DynamicPropertyValueSerializer.get();

        Object actual = converter.fromByteBuffer(converter.toByteBuffer(value));
        if (value.getClass() == byte[].class)
            Assert.assertArrayEquals((byte[]) value, (byte[]) actual);
        else
            Assert.assertEquals(value, actual);
    }

}

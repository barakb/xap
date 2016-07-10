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


package com.j_spaces.jdbc;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.HashMap;

/**
 * Utility for handling number functions
 *
 * @author anna
 * @version 1.0
 * @since 5.1
 */
@com.gigaspaces.api.InternalApi
public class NumberUtil {

    private static HashMap<String, NumberHandler> _numberMap = new HashMap<String, NumberHandler>();


    static {
        _numberMap.put(Integer.class.getName(), new IntegerHandler());
        _numberMap.put(Long.class.getName(), new LongHandler());
        _numberMap.put(Double.class.getName(), new DoubleHandler());
        _numberMap.put(Float.class.getName(), new FloatHandler());
        _numberMap.put(Short.class.getName(), new ShortHandler());
        _numberMap.put(Byte.class.getName(), new ByteHandler());
        _numberMap.put(BigDecimal.class.getName(), new BigDecimalHandler());
        _numberMap.put(BigInteger.class.getName(), new BigIntegerHandler());
        _numberMap.put(int.class.getName(), new IntegerHandler());
        _numberMap.put(long.class.getName(), new LongHandler());
        _numberMap.put(double.class.getName(), new DoubleHandler());
        _numberMap.put(float.class.getName(), new FloatHandler());
        _numberMap.put(short.class.getName(), new ShortHandler());
        _numberMap.put(byte.class.getName(), new ByteHandler());


    }

    public static Number cast(Number x, String numberClassName) {
        NumberHandler handler = getHandler(numberClassName);

        return handler.cast(x);
    }

    public static Number add(Number x, Number y, String numberClassName) {
        NumberHandler handler = getHandler(numberClassName);

        return handler.add(x, y);
    }

    public static Number subtract(Number x, Number y, String numberClassName) {
        NumberHandler handler = getHandler(numberClassName);
        return handler.subtract(x, y);
    }

    public static Number mul(Number x, Number y, String numberClassName) {
        NumberHandler handler = getHandler(numberClassName);
        return handler.mul(x, y);
    }

    public static Number divide(Number x, Number y, String numberClassName) {
        NumberHandler handler = getHandler(numberClassName);

        return handler.divide(x, y);
    }


    private static NumberHandler getHandler(String numberClassName) {
        NumberHandler handler = _numberMap.get(numberClassName);

        if (handler == null)
            throw new IllegalArgumentException("Invalid number class - [" + numberClassName + "]");

        return handler;
    }

    /**
     * Handles number operations
     *
     * @author anna
     * @version 1.0
     * @since 5.1
     */
    private interface NumberHandler {
        Number cast(Number number);

        Number add(Number x, Number y);

        Number subtract(Number x, Number y);

        Number mul(Number x, Number y);

        Number divide(Number x, Number y);
    }

    /**
     * Handler for integer values
     *
     * @author anna
     * @version 1.0
     * @since 5.1
     */
    private static class IntegerHandler implements NumberHandler {

        public Integer cast(Number number) {
            return cast0(number);
        }

        private int cast0(Number number) {
            return number.intValue();
        }

        /*
         * @see com.j_spaces.jdbc.NumberUtil.NumberHandler#divide(java.lang.Number, java.lang.Number)
         */
        public Number divide(Number x, Number y) {
            return cast0(x) / cast0(y);
        }

        /*
         * @see com.j_spaces.jdbc.NumberUtil.NumberHandler#minus(java.lang.Number, java.lang.Number)
         */
        public Number subtract(Number x, Number y) {
            return cast0(x) - cast0(y);
        }

        /*
         * @see com.j_spaces.jdbc.NumberUtil.NumberHandler#mul(java.lang.Number, java.lang.Number)
         */
        public Number mul(Number x, Number y) {
            return cast0(x) * cast0(y);
        }

        /*
         * @see com.j_spaces.jdbc.NumberUtil.NumberHandler#plus(java.lang.Number, java.lang.Number)
         */
        public Number add(Number x, Number y) {
            return cast0(x) + cast0(y);
        }
    }


    /**
     * Handler for long values
     *
     * @author anna
     * @version 1.0
     * @since 5.1
     */
    private static class LongHandler implements NumberHandler {
        public Long cast(Number number) {
            return cast0(number);
        }

        private long cast0(Number number) {
            return number.longValue();
        }

        /*
         * @see com.j_spaces.jdbc.NumberUtil.NumberHandler#divide(java.lang.Number, java.lang.Number)
         */
        public Number divide(Number x, Number y) {
            return cast0(x) / cast0(y);
        }

        /*
         * @see com.j_spaces.jdbc.NumberUtil.NumberHandler#minus(java.lang.Number, java.lang.Number)
         */
        public Number subtract(Number x, Number y) {
            return cast0(x) - cast0(y);
        }

        /*
         * @see com.j_spaces.jdbc.NumberUtil.NumberHandler#mul(java.lang.Number, java.lang.Number)
         */
        public Number mul(Number x, Number y) {
            return cast0(x) * cast0(y);
        }

        /*
         * @see com.j_spaces.jdbc.NumberUtil.NumberHandler#plus(java.lang.Number, java.lang.Number)
         */
        public Number add(Number x, Number y) {
            return cast0(x) + cast0(y);
        }


    }

    /**
     * Handler for double values
     *
     * @author anna
     * @version 1.0
     * @since 5.1
     */
    private static class DoubleHandler implements NumberHandler {

        public Double cast(Number number) {
            return cast0(number);
        }

        private double cast0(Number number) {
            return number.doubleValue();
        }

        /*
         * @see com.j_spaces.jdbc.NumberUtil.NumberHandler#divide(java.lang.Number, java.lang.Number)
         */
        public Number divide(Number x, Number y) {
            return cast0(x) / cast0(y);
        }

        /*
         * @see com.j_spaces.jdbc.NumberUtil.NumberHandler#minus(java.lang.Number, java.lang.Number)
         */
        public Number subtract(Number x, Number y) {
            return cast0(x) - cast0(y);
        }

        /*
         * @see com.j_spaces.jdbc.NumberUtil.NumberHandler#mul(java.lang.Number, java.lang.Number)
         */
        public Number mul(Number x, Number y) {
            return cast0(x) * cast0(y);
        }

        /*
         * @see com.j_spaces.jdbc.NumberUtil.NumberHandler#plus(java.lang.Number, java.lang.Number)
         */
        public Number add(Number x, Number y) {
            return cast0(x) + cast0(y);
        }


    }


    /**
     * Handler for float values
     *
     * @author anna
     * @version 1.0
     * @since 5.1
     */
    private static class FloatHandler implements NumberHandler {
        public Float cast(Number number) {
            return cast0(number);
        }

        private float cast0(Number number) {
            return number.floatValue();
        }

        /*
         * @see com.j_spaces.jdbc.NumberUtil.NumberHandler#divide(java.lang.Number, java.lang.Number)
         */
        public Number divide(Number x, Number y) {
            return cast0(x) / cast0(y);
        }

        /*
         * @see com.j_spaces.jdbc.NumberUtil.NumberHandler#minus(java.lang.Number, java.lang.Number)
         */
        public Number subtract(Number x, Number y) {
            return cast0(x) - cast0(y);
        }

        /*
         * @see com.j_spaces.jdbc.NumberUtil.NumberHandler#mul(java.lang.Number, java.lang.Number)
         */
        public Number mul(Number x, Number y) {
            return cast0(x) * cast0(y);
        }

        /*
         * @see com.j_spaces.jdbc.NumberUtil.NumberHandler#plus(java.lang.Number, java.lang.Number)
         */
        public Number add(Number x, Number y) {
            return cast0(x) + cast0(y);
        }


    }


    /**
     * Handler for short values
     *
     * @author anna
     * @version 1.0
     * @since 5.1
     */
    private static class ShortHandler implements NumberHandler {

        public Short cast(Number number) {
            return cast0(number);
        }

        public short cast0(Number number) {
            return number.shortValue();
        }

        /*
         * @see com.j_spaces.jdbc.NumberUtil.NumberHandler#divide(java.lang.Number, java.lang.Number)
         */
        public Number divide(Number x, Number y) {
            return cast0(x) / cast0(y);
        }

        /*
         * @see com.j_spaces.jdbc.NumberUtil.NumberHandler#minus(java.lang.Number, java.lang.Number)
         */
        public Number subtract(Number x, Number y) {
            return cast0(x) - cast0(y);
        }

        /*
         * @see com.j_spaces.jdbc.NumberUtil.NumberHandler#mul(java.lang.Number, java.lang.Number)
         */
        public Number mul(Number x, Number y) {
            return cast0(x) * cast0(y);
        }

        /*
         * @see com.j_spaces.jdbc.NumberUtil.NumberHandler#plus(java.lang.Number, java.lang.Number)
         */
        public Number add(Number x, Number y) {
            return cast0(x) + cast0(y);
        }


    }

    /**
     * Handler for byte values
     *
     * @author anna
     * @version 1.0
     * @since 5.1
     */
    private static class ByteHandler implements NumberHandler {
        public Byte cast(Number number) {
            return cast0(number);
        }

        private byte cast0(Number number) {
            return number.byteValue();
        }

        /*
         * @see com.j_spaces.jdbc.NumberUtil.NumberHandler#divide(java.lang.Number, java.lang.Number)
         */
        public Number divide(Number x, Number y) {
            return cast0(x) / cast0(y);
        }

        /*
         * @see com.j_spaces.jdbc.NumberUtil.NumberHandler#minus(java.lang.Number, java.lang.Number)
         */
        public Number subtract(Number x, Number y) {
            return cast0(x) - cast0(y);
        }

        /*
         * @see com.j_spaces.jdbc.NumberUtil.NumberHandler#mul(java.lang.Number, java.lang.Number)
         */
        public Number mul(Number x, Number y) {
            return cast0(x) * cast0(y);
        }

        /*
         * @see com.j_spaces.jdbc.NumberUtil.NumberHandler#plus(java.lang.Number, java.lang.Number)
         */
        public Number add(Number x, Number y) {
            return cast0(x) + cast0(y);
        }
    }


    /**
     * Handler for big decimal values
     *
     * @author anna
     * @version 1.0
     * @since 5.1
     */
    private static class BigDecimalHandler implements NumberHandler {

        public BigDecimal cast(Number number) {
            return (BigDecimal) number;
        }

        /*
         * @see com.j_spaces.jdbc.NumberUtil.NumberHandler#divide(java.lang.Number, java.lang.Number)
         */
        public Number divide(Number x, Number y) {
            return cast(x).divide(cast(y));
        }

        /*
         * @see com.j_spaces.jdbc.NumberUtil.NumberHandler#minus(java.lang.Number, java.lang.Number)
         */
        public Number subtract(Number x, Number y) {
            return cast(x).subtract(cast(y));
        }

        /*
         * @see com.j_spaces.jdbc.NumberUtil.NumberHandler#mul(java.lang.Number, java.lang.Number)
         */
        public Number mul(Number x, Number y) {
            return cast(x).multiply(cast(y));
        }

        /*
         * @see com.j_spaces.jdbc.NumberUtil.NumberHandler#plus(java.lang.Number, java.lang.Number)
         */
        public Number add(Number x, Number y) {
            return cast(x).add(cast(y));
        }


    }

    /**
     * Handler for big integer values
     *
     * @author anna
     * @version 1.0
     * @since 5.1
     */
    private static class BigIntegerHandler implements NumberHandler {

        public BigInteger cast(Number number) {
            return (BigInteger) number;
        }

        /*
         * @see com.j_spaces.jdbc.NumberUtil.NumberHandler#divide(java.lang.Number, java.lang.Number)
         */
        public Number divide(Number x, Number y) {
            return cast(x).divide(cast(y));
        }

        /*
         * @see com.j_spaces.jdbc.NumberUtil.NumberHandler#minus(java.lang.Number, java.lang.Number)
         */
        public Number subtract(Number x, Number y) {
            return cast(x).subtract(cast(y));
        }

        /*
         * @see com.j_spaces.jdbc.NumberUtil.NumberHandler#mul(java.lang.Number, java.lang.Number)
         */
        public Number mul(Number x, Number y) {
            return cast(x).multiply(cast(y));
        }

        /*
         * @see com.j_spaces.jdbc.NumberUtil.NumberHandler#plus(java.lang.Number, java.lang.Number)
         */
        public Number add(Number x, Number y) {
            return cast(x).add(cast(y));
        }


    }
}

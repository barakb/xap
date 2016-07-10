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

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import me.prettyprint.hector.api.ddl.ComparatorType;

/**
 * Helper class to deal with validators that hector doesn't take into consideration
 *
 * @author Dan Kilman
 * @since 9.1.1
 */
public class ValidatorClassInferer {

    // These should be removed when hector finally includes them as part of the serializer
    public static final ComparatorType INT32TYPE = type("Int32Type");
    public static final ComparatorType DATETYPE = type("DateType");
    public static final ComparatorType BOOLEANTYPE = type("BooleanType");
    public static final ComparatorType FLOATTYPE = type("FloatType");
    public static final ComparatorType DOUBLETYPE = type("DoubleType");

    private static ComparatorType type(String name) {
        return ComparatorType.getByClassName("org.apache.cassandra.db.marshal." + name);
    }

    private static final Map<Class<?>, String> validators = new HashMap<Class<?>, String>();

    static {
        validators.put(boolean.class, BOOLEANTYPE.getClassName());
        validators.put(Boolean.class, BOOLEANTYPE.getClassName());
        validators.put(int.class, INT32TYPE.getClassName());
        validators.put(Integer.class, INT32TYPE.getClassName());
        validators.put(float.class, FLOATTYPE.getClassName());
        validators.put(Float.class, FLOATTYPE.getClassName());
        validators.put(double.class, DOUBLETYPE.getClassName());
        validators.put(Double.class, DOUBLETYPE.getClassName());
        validators.put(Date.class, DATETYPE.getClassName());
    }

    /**
     * @param type The type to infer from.
     * @return The matching Cassandra validation class.
     */
    public static String infer(Class<?> type) {
        String validationClass = validators.get(type);
        if (validationClass == null)
            validationClass = SerializerProvider.getSerializer(type).getComparatorType().getClassName();

        return validationClass;
    }

    /**
     * @return The default bytes type Cassandra validation class.
     */
    public static String getBytesTypeValidationClass() {
        return ComparatorType.BYTESTYPE.getClassName();
    }

    /**
     * @param type The type to test.
     * @return <code>true</code> if this type matches the default bytes type Cassandra validation
     * class.
     */
    public static boolean isBytesType(Class<?> type) {
        return ComparatorType.BYTESTYPE.getClassName().equals(infer(type));
    }

}

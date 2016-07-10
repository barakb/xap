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

package org.openspaces.persistency.cassandra.meta.conversion;

import com.gigaspaces.security.encoding.md5.Md5Encrypter;

/**
 * A {@link ColumnFamilyNameConverter} implementation that works as follows: If the type names
 * length does not exceed a 48 characters length, the dots in the type name will be replaced by
 * underscores. Otherwise, the type simple name (without the packages prefix) will be used with the
 * first 6 chars of the string representation of the given type name MD5 hash to avoid name
 * collisions. If the above also exceeds a 48 characters length, the simple name will be truncated
 * to its first 42 characters and concatenated with the MD5 suffix mentioned above.
 *
 * @author Dan Kilman
 * @since 9.1.1
 */
public class DefaultColumnFamilyNameConverter implements ColumnFamilyNameConverter {

    private static final int MAX_CF_NAME_LENGTH = 48;
    private static final int NUMBER_OF_MD5_CHARS_SUFFIX = 6;

    private final Md5Encrypter encrypter = new Md5Encrypter();

    @Override
    public String toColumnFamilyName(String typeName) {
        if (typeName == null) {
            return null;
        }

        String typeNameMD5 = encrypter.encrypt(typeName);
        String typeNameMD5Suffix = typeNameMD5.substring(0, NUMBER_OF_MD5_CHARS_SUFFIX);

        String underScoredTypeName = typeName.replace('.', '_').replace('$', '_');
        String[] typeNameSplit = underScoredTypeName.split("_");
        String simpleTypeName = typeNameSplit[typeNameSplit.length - 1];

        String columnFamilyName;
        if (underScoredTypeName.length() <= MAX_CF_NAME_LENGTH) {
            columnFamilyName = underScoredTypeName;
        } else if (simpleTypeName.length() + NUMBER_OF_MD5_CHARS_SUFFIX <= MAX_CF_NAME_LENGTH) {
            columnFamilyName = simpleTypeName + typeNameMD5Suffix;
        } else {
            columnFamilyName =
                    simpleTypeName.substring(0, MAX_CF_NAME_LENGTH - NUMBER_OF_MD5_CHARS_SUFFIX) + typeNameMD5Suffix;
        }

        return columnFamilyName;
    }

}

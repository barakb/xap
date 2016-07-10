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

//Internal Doc
package com.gigaspaces.security.encoding.md5;

import com.gigaspaces.security.encoding.EncodingException;
import com.gigaspaces.security.encoding.PasswordEncoder;

/**
 * Encodes passwords using the MD5 algorithm. The encoded password is returned as Hex (32 char)
 * version of the hash bytes.
 *
 * @author Moran Avigdor
 * @since 7.0.1
 */
@com.gigaspaces.api.InternalApi
public class Md5PasswordEncoder implements PasswordEncoder {

    private final Md5Encrypter encrypter = new Md5Encrypter();

    /*
     * @see
     * com.gigaspaces.security.fs.encoding.PasswordEncoder#encodePassword(java.lang.String)
     */
    public String encodePassword(String rawPass) throws EncodingException {
        return encrypter.encrypt(rawPass);
    }

    /*
     * @see
     * com.gigaspaces.security.fs.encoding.PasswordEncoder#isPasswordValid(java.lang.String,
     * java.lang.String)
     */
    public boolean isPasswordValid(String encPass, String rawPass) throws EncodingException {
        return encPass.equals(encodePassword(rawPass));
    }
}

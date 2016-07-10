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
package com.gigaspaces.security.encoding.aes;

import com.gigaspaces.security.encoding.EncodingException;

import java.security.spec.AlgorithmParameterSpec;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;

/**
 * AES is a federal standard for private-key or symmetric cryptography. It supports combinations of
 * key and block sizes of 128, 192, and 256.
 *
 * @author Moran Avigdor
 * @since 7.0.1
 */
@com.gigaspaces.api.InternalApi
public class AesEncrypter {
    Cipher ecipher;
    Cipher dcipher;

    /**
     * Constructs an AES Encrypter instance with the provided secret key.
     *
     * @param key a secret key to use to construct the {@link Cipher}s needed to encrypt and
     *            decrypt.
     */
    public AesEncrypter(SecretKey key) {

        // Create an 8-byte initialization vector
        byte[] iv = new byte[]{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d,
                0x0e, 0x0f};

        AlgorithmParameterSpec paramSpec = new IvParameterSpec(iv);

        try {
            ecipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
            dcipher = Cipher.getInstance("AES/CBC/PKCS5Padding");

            // CBC requires an initialization vector
            ecipher.init(Cipher.ENCRYPT_MODE, key, paramSpec);
            dcipher.init(Cipher.DECRYPT_MODE, key, paramSpec);
        } catch (Exception e) {
            throw new EncodingException("Failed to create AES encrypter", e);
        }
    }
}

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

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Uses {@link MessageDigest} which provides applications the functionality of a message digest
 * algorithm, such as MD5. Message digests are secure one-way hash functions that take
 * arbitrary-sized data and output a fixed-length hash value.
 *
 * @author Moran Avigdor
 * @since 7.0.1
 */
@com.gigaspaces.api.InternalApi
public class Md5Encrypter {
    private final static String algorithm = "MD5";
    private final MessageDigest messageDigest = getMessageDigest();

    /**
     * Get a MessageDigest instance for the given algorithm. Throws an IllegalArgumentException if
     * <i>algorithm</i> is unknown
     *
     * @return MessageDigest instance
     * @throws EncodingException if NoSuchAlgorithmException is thrown
     */
    private MessageDigest getMessageDigest() {
        try {
            return MessageDigest.getInstance(algorithm);
        } catch (NoSuchAlgorithmException e) {
            throw new EncodingException("No such algorithm [" + algorithm + "]", e);
        }
    }

    /**
     * Encrypts the raw String using the MD5 algorithm.
     *
     * @param raw a raw String to encrypt
     */
    public String encrypt(String raw) {
        try {
            MessageDigest localMessageDigest = cloneMessageDigest();
            byte[] digest = localMessageDigest.digest(raw.getBytes("UTF-8"));
            return new String(Hex.encodeHex(digest));
        } catch (UnsupportedEncodingException e) {
            throw new EncodingException("UTF-8 not supported!", e);
        }
    }

    /**
     * Clone Message Digest to support simultaneous requests.
     *
     * @return A clone or a new digest if clone is not supported.
     */
    private MessageDigest cloneMessageDigest() {
        try {
            return (MessageDigest) messageDigest.clone();
        } catch (CloneNotSupportedException e) {
            return getMessageDigest();
        }
    }

    /**
     * Hex encoder.
     */
    private final static class Hex {

        /**
         * Converts an array of bytes into an array of characters representing the hexadecimal
         * values of each byte in order. The returned array will be double the length of the passed
         * array, as it takes two characters to represent any given byte.
         *
         * @param data a byte[] to convert to Hex characters
         * @return A char[] containing hexadecimal characters
         */
        public static String encodeHex(byte[] data) {
            StringBuffer buf = new StringBuffer();
            for (int i = 0; i < data.length; i++) {
                int halfbyte = (data[i] >>> 4) & 0x0F;
                int two_halfs = 0;
                do {
                    if ((0 <= halfbyte) && (halfbyte <= 9))
                        buf.append((char) ('0' + halfbyte));
                    else
                        buf.append((char) ('a' + (halfbyte - 10)));
                    halfbyte = data[i] & 0x0F;
                } while (two_halfs++ < 1);
            }
            return buf.toString();
        }
    }
}

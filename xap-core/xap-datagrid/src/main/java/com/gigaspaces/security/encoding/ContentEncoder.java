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


package com.gigaspaces.security.encoding;

/**
 * Interface for performing two-way encryption and decryption on objects.
 *
 * @author Moran Avigdor
 * @since 7.0.1
 */
public interface ContentEncoder {

    /**
     * Encode the object returning an encrypted byte array.
     *
     * @param obj Object to encode.
     * @return a byte array representing the encoded object.
     * @throws EncodingException if failed to encode the object.
     */
    public byte[] encode(Object obj) throws EncodingException;

    /**
     * Decode the byte array returning a decrypted object.
     *
     * @param bytes a byte array representing the encoded object.
     * @return the decoded object.
     * @throws EncodingException if failed to decode the object.
     */
    public Object decode(byte[] bytes) throws EncodingException;
}

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

package com.gigaspaces.serialization.pbs;

import java.lang.ref.SoftReference;

/**
 * Supply reused PbsStream buffers.
 *
 * @author eitany
 * @since 7.0
 */
@com.gigaspaces.api.InternalApi
public class PbsStreamResource {
    private final static int POOL_SIZE = Integer.getInteger("com.gs.dotnet.stream-pool-size", 3);

    private final static ThreadLocal<SoftReference<PbsOutputStream>[]> _outputStream = new ThreadLocal<SoftReference<PbsOutputStream>[]>() {
        @Override
        protected SoftReference<PbsOutputStream>[] initialValue() {
            SoftReference<PbsOutputStream>[] pool = new SoftReference[POOL_SIZE];
            for (int i = 0; i < POOL_SIZE; i++)
                pool[i] = new SoftReference<PbsOutputStream>(new PbsOutputStream());

            return pool;
        }
    };

    private final static ThreadLocal<SoftReference<PbsInputStream>[]> _inputStream = new ThreadLocal<SoftReference<PbsInputStream>[]>() {
        @Override
        protected SoftReference<PbsInputStream>[] initialValue() {
            SoftReference<PbsInputStream>[] pool = new SoftReference[POOL_SIZE];
            for (int i = 0; i < POOL_SIZE; i++) {
                pool[i] = new SoftReference<PbsInputStream>(null);
            }
            return pool;
        }
    };

    /**
     * Get a PbsOutputStream
     *
     * @return a PbsOutputStream
     */
    public static PbsOutputStream getOutputStream() {
        SoftReference<PbsOutputStream>[] pool = _outputStream.get();
        for (int i = 0; i < POOL_SIZE; i++) {
            SoftReference<PbsOutputStream> softReference = pool[i];
            PbsOutputStream output = softReference.get();
            if (output == null) {
                output = new PbsOutputStream();
                pool[i] = new SoftReference<PbsOutputStream>(output);
                output.setAsUsed();
                return output;
            } else if (output.isUsed()) {
                continue;
            } else {
                output.reset();
                output.setAsUsed();
                return output;
            }

        }
        //Scanned all pool and did not find an available resource, create a temp new one.
        PbsOutputStream output = new PbsOutputStream();
        output.setAsUsed();
        return output;
    }

    /**
     * Release the used output stream
     */
    public static void releasePbsStream(IThreadLocalResource pbsStream) {
        pbsStream.release();
    }

    /**
     * Get a PbsInputStream
     *
     * @param buffer to wrap with an input stream
     * @return a PbsInputStream
     */
    public static PbsInputStream getInputStream(byte[] buffer) {
        SoftReference<PbsInputStream>[] pool = _inputStream.get();
        for (int i = 0; i < POOL_SIZE; i++) {
            SoftReference<PbsInputStream> softReference = pool[i];
            PbsInputStream input = softReference.get();
            if (input == null) {
                input = new PbsInputStream(buffer);
                pool[i] = new SoftReference<PbsInputStream>(input);
                input.setAsUsed();
                return input;
            } else if (input.isUsed()) {
                continue;
            } else {
                input.setBuffer(buffer);
                input.setAsUsed();
                return input;
            }

        }
        //Scanned all pool and did not find an available resource, create a temp new one.
        PbsInputStream input = new PbsInputStream(buffer);
        input.setAsUsed();
        return input;
    }

    public static PbsInputStream getExistingInputStream() {
        SoftReference<PbsInputStream>[] pool = _inputStream.get();
        for (int i = 0; i < POOL_SIZE; i++) {
            SoftReference<PbsInputStream> softReference = pool[i];
            PbsInputStream input = softReference.get();
            if (input == null) {
                continue;
            } else if (input.isUsed()) {
                continue;
            } else {
                input.setAsUsed();
                input.reset();
                return input;
            }

        }
        return null;
    }
}

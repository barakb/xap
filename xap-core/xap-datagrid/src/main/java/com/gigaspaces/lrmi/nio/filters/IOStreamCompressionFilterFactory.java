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


package com.gigaspaces.lrmi.nio.filters;

import com.gigaspaces.lrmi.nio.filters.IOStreamCompressionFilter.Algo;

import java.net.InetSocketAddress;
import java.util.zip.Deflater;

/**
 * Use this network filter factory to send compress messages between client and server.
 *
 * @author barak
 */

public class IOStreamCompressionFilterFactory implements
        IOFilterFactory {

    public IOStreamCompressionFilterFactory() {
    }

    public IOFilter createFilter() throws Exception {
        // compresson level should be a number in (0-9)
        Integer compressionLevel = Integer.getInteger("COMPRESSION",
                Deflater.BEST_COMPRESSION);
        IOStreamCompressionFilter streamCompressionFilter = new IOStreamCompressionFilter(
                Algo.ZIP);
        streamCompressionFilter.setCompressionLevel(compressionLevel);
        return streamCompressionFilter;
    }

    public IOFilter createClientFilter(InetSocketAddress remoteAddress) throws Exception {
        return createFilter();
    }

    public IOFilter createServerFilter(InetSocketAddress remoteAddress) throws Exception {
        return createFilter();
    }

}
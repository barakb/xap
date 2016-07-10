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

package com.gigaspaces.logger;

import java.io.IOException;
import java.io.OutputStream;

/**
 * A metered stream is a subclass of OutputStream that:
 *
 * <pre>
 * (a) forwards all its output to a target stream
 * (b) keeps track of how many bytes have been written
 * </pre>
 *
 * It is used to meter the limit of a log file.
 *
 * @author Moran Avigdor
 * @since 7.0
 */
class MeteredStream extends OutputStream {
    OutputStream out;
    int written;

    MeteredStream(OutputStream out, int written) {
        this.out = out;
        this.written = written;
    }

    @Override
    public void write(int b) throws IOException {
        out.write(b);
        written++;
    }

    @Override
    public void write(byte buff[]) throws IOException {
        out.write(buff);
        written += buff.length;
    }

    @Override
    public void write(byte buff[], int off, int len) throws IOException {
        out.write(buff, off, len);
        written += len;
    }

    @Override
    public void flush() throws IOException {
        out.flush();
    }

    @Override
    public void close() throws IOException {
        out.close();
    }
}

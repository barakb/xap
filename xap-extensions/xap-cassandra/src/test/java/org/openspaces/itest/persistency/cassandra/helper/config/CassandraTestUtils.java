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

package org.openspaces.itest.persistency.cassandra.helper.config;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class CassandraTestUtils {

    public static void writeToFile(File file, String content) throws IOException {
        FileWriter writer = null;
        try {
            writer = new FileWriter(file);
            writer.write(content);
        } finally {
            if (writer != null)
                writer.close();
        }
    }

    public static void deleteFileOrDirectory(File fileOrDirectory) throws IOException {
        if (fileOrDirectory.isDirectory()) {
            for (File file : fileOrDirectory.listFiles())
                deleteFileOrDirectory(file);
        }
        if (!fileOrDirectory.delete()) {
            throw new IOException("Failed deleting " + fileOrDirectory);
        }
    }
}

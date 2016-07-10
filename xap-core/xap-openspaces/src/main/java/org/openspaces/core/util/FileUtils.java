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

package org.openspaces.core.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jini.rio.boot.PUZipUtils;

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

/**
 * @author itaif
 * @since 9.0.1 TODO: Replace with commons-io once it is approved as an openspaces dependency
 */
public class FileUtils {

    private static final Log logger = LogFactory.getLog(FileUtils.class);

    public static void deleteFileOrDirectory(File fileOrDirectory) {
        if (!com.gigaspaces.internal.io.FileUtils.deleteFileOrDirectoryIfExists(fileOrDirectory)) {
            throw new RuntimeException("Failed to delete " + fileOrDirectory);
        }
    }

    /**
     * unzips the specified zip file to a temp folder
     *
     * @return the new temp folder
     * @since 9.0.1
     */
    public static File unzipToTempFolder(File zipFile) {
        String zipFilename = zipFile.getName();
        String tempFolderPrefix = zipFilename.substring(0, zipFilename.lastIndexOf('.'));
        File tempFolder = createTempFolder(tempFolderPrefix);
        try {
            PUZipUtils.unzip(zipFile, tempFolder);
            return tempFolder;
        } catch (Exception e) {
            try {
                FileUtils.deleteFileOrDirectory(tempFolder);
            } catch (RuntimeException ex) {
                logger.debug("Failed to delete folder " + tempFolder, ex);
            }
            throw new RuntimeException("Failed to unzip file " + zipFile + " to " + tempFolder, e);
        }
    }

    public static File createTempFolder(String tempFolderPrefix) {
        File tempFile;
        try {
            tempFile = File.createTempFile("unzip_" + tempFolderPrefix.replace('.', '_'), "");
        } catch (final IOException e) {
            throw new RuntimeException("Failed to create temp file with prefix " + tempFolderPrefix, e);
        }
        FileUtils.deleteFileOrDirectory(tempFile);

        final boolean created = tempFile.mkdirs();
        if (!created) {
            throw new RuntimeException("Failed to create temp file " + tempFile);
        }
        return tempFile;
    }

    public static byte[] unzipFileToMemory(String applicationFile, File directoryOrZip, long maxFileSizeInBytes) {
        ZipFile zipFile = null;
        try {
            zipFile = new ZipFile(directoryOrZip);
            final ZipEntry zipEntry = zipFile.getEntry(applicationFile);
            if (zipEntry == null) {
                throw new RuntimeException("Failed to load " + applicationFile + " from " + directoryOrZip);
            }
            final int length = (int) zipEntry.getSize();
            if (length > maxFileSizeInBytes) {
                throw new RuntimeException(applicationFile + " file size cannot be bigger than " + maxFileSizeInBytes
                        + " bytes");
            }
            byte[] buffer = new byte[length];
            final InputStream in = zipFile.getInputStream(zipEntry);
            final DataInputStream din = new DataInputStream(in);
            try {
                din.readFully(buffer, 0, length);
                return buffer;
            } catch (final IOException e) {
                throw new RuntimeException("Failed to read application file input stream", e);
            }

        } catch (final IOException e) {
            throw new RuntimeException("Failed to read zip file " + directoryOrZip, e);
        } finally {
            if (zipFile != null) {
                try {
                    zipFile.close();
                } catch (final IOException e) {
                    logger.debug("Failed to close zip file " + directoryOrZip, e);
                }
            }
        }
    }

}

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

package com.gigaspaces.start;

import com.gigaspaces.internal.io.BootIOUtils;

import java.io.File;
import java.io.FileFilter;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by niv on 5/24/2016.
 */
@com.gigaspaces.api.InternalApi
public class ClasspathBuilder {

    private final List<File> files = new ArrayList<File>();

    public ClasspathBuilder append(String path) {
        return append(path, null);
    }

    public ClasspathBuilder append(String path, FileFilter filter) {
        final JarFileFilter jarFileFilter = new JarFileFilter(filter);
        File f = new File(path);
        if (f.isDirectory()) {
            final File[] files = BootIOUtils.listFiles(f, jarFileFilter);
            for (File file : files)
                this.files.add(file);
        } else {
            if (filter == null || filter.accept(f))
                files.add(f);
        }
        return this;
    }

    public List<URL> toURLs() throws MalformedURLException {
        List<URL> result = new ArrayList<URL>();
        for (File file : files)
            result.add(file.toURI().toURL());
        return result;
    }

    public List<String> toFilesNames() {
        List<String> result = new ArrayList<String>();
        for (File file : files)
            result.add(file.getAbsolutePath());
        return result;
    }

    private static class JarFileFilter implements FileFilter {

        private final FileFilter secondaryFilter;

        private JarFileFilter(FileFilter secondaryFilter) {
            this.secondaryFilter = secondaryFilter;
        }

        @Override
        public boolean accept(File pathname) {
            String filename = pathname.getName().toLowerCase();
            if (filename.endsWith(".jar") || filename.endsWith(".zip")) {
                return secondaryFilter != null ? secondaryFilter.accept(pathname) : true;
            } else {
                return false;
            }
        }
    }
}

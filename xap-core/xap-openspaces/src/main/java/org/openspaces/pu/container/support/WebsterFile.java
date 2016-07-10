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


package org.openspaces.pu.container.support;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

/**
 * Not an actual file. Simply wraps a URL over webster (GSM http server) inorder to allow to utilize
 * listing of "files" over webster.
 *
 * @author kimchy
 */
public class WebsterFile extends File {

    private static final long serialVersionUID = 6166390927496427656L;

    private URL root;

    private URL url;

    private boolean directory = true;

    private long time = -1;

    private String name;

    public WebsterFile(URL url) {
        super("");
        this.root = url;
        this.url = url;
    }

    WebsterFile(URL root, URL url, String name, long time, boolean directory) throws MalformedURLException {
        super("");
        String fullUrl = url.toExternalForm();
        if (!fullUrl.endsWith("/")) {
            fullUrl += "/" + name;
        } else {
            fullUrl += name;
        }
        this.root = root;
        this.url = new URL(fullUrl);
        this.time = time;
        this.directory = directory;
        this.name = name;
    }

    public String getName() {
        if (name == null) {
            return super.getName();
        }
        return name;
    }

    public URL toURL() throws MalformedURLException {
        return this.url;
    }

    public String getPath() {
        return url.toExternalForm().substring(root.toExternalForm().length());
    }

    public String getAbsolutePath() {
        return url.toExternalForm();
    }

    public File getAbsoluteFile() {
        return this;
    }

    public boolean exists() {
        return true;
    }

    public boolean isDirectory() {
        return directory;
    }

    public boolean isAbsolute() {
        return true;
    }

    public File[] listFiles() {
        String line;
        try {
            List<File> filesList = new ArrayList<File>();
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setDoOutput(false);
            conn.setDoInput(true);
            conn.setAllowUserInteraction(false);
            conn.setUseCaches(false);
            conn.setRequestMethod("GET");
            conn.setRequestProperty("list", "true");
            conn.connect();

            BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream()));
            while ((line = reader.readLine()) != null) {
                StringTokenizer tokenizer = new StringTokenizer(line, "\t");
                String name = tokenizer.nextToken();
                String type = tokenizer.nextToken();
                String size = tokenizer.nextToken();
                long time = Long.parseLong(tokenizer.nextToken());
                File add = new WebsterFile(root, url, name, time, type.equals("d"));
                filesList.add(add);
            }
            reader.close();
            try {
                conn.disconnect();
            } catch (Exception e) {
                // no matter, failed to close
            }
            return filesList.toArray(new File[filesList.size()]);
        } catch (Exception e) {
            return new File[0];
        }
    }

    public boolean isFile() {
        return !directory;
    }

    public long lastModified() {
        return time;
    }
}

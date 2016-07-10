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

import org.springframework.core.io.UrlResource;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;

/**
 * A webster resource which holds a url to an actual content stored on GSM webster. Extends {@link
 * org.springframework.core.io.UrlResource} in order to allow to return {@link
 * org.openspaces.pu.container.support.WebsterFile} for {@link #getFile()} method.
 *
 * <p>{@link org.openspaces.pu.container.support.WebsterFile} allows to list "files" over webster
 * http service. This allows to perform different scanning over the processing unit content.
 *
 * @author kimchy
 */
public class WebsterResoruce extends UrlResource {

    public WebsterResoruce(URL url) {
        super(url);
    }

    public WebsterResoruce(URI uri) throws MalformedURLException {
        super(uri);
    }

    public WebsterResoruce(String path) throws MalformedURLException {
        super(path);
    }

    /**
     * Returns a {@link org.openspaces.pu.container.support.WebsterFile} over the given URL.
     */
    public File getFile() throws IOException {
        return new WebsterFile(getURL());
    }
}

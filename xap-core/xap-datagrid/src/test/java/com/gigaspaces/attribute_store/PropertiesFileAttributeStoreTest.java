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

package com.gigaspaces.attribute_store;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.Properties;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.nullValue;

/**
 * Created by Barak Bar Orion 7/2/15.
 */
@com.gigaspaces.api.InternalApi
public class PropertiesFileAttributeStoreTest {
    private String path;
    private PropertiesFileAttributeStore attributeStore1;
    private PropertiesFileAttributeStore attributeStore2;


    @After
    public void tearDown() throws IOException {
        //noinspection ResultOfMethodCallIgnored
        new File(path).delete();
        //noinspection ResultOfMethodCallIgnored
        new File(path).getParentFile().delete();
    }

    @Before
    public void setUp() throws IOException {
        path = UUID.randomUUID().toString() + "/" + UUID.randomUUID().toString();
        attributeStore1 = new PropertiesFileAttributeStore(path);
        attributeStore2 = new PropertiesFileAttributeStore(path);
    }

    @Test
    public void testSetGet() throws IOException {
        attributeStore1.set("foo", "bar");
        attributeStore2.set("foo1", "bar1");
        assertThat(attributeStore1.get("foo"), is("bar"));
        assertThat(attributeStore2.get("foo"), is("bar"));
    }

    @Test
    public void testSetSetGet() throws IOException {
        String old = attributeStore1.get("foo");
        assertThat(old, is(nullValue()));
        old = attributeStore1.set("foo", "barbar");
        assertThat(old, is(nullValue()));
        old = attributeStore1.set("foo", "bar");
        assertThat(old, is("barbar"));
        assertThat(attributeStore1.get("foo"), is("bar"));
    }

    @Test
    public void testCreateOnDirectory() throws Exception {
        File file = new File(UUID.randomUUID().toString() + "/" + UUID.randomUUID().toString());
        File dir = file.getParentFile();
        try {
            assertThat(dir.mkdirs(), is(true));
            PropertiesFileAttributeStore attributeStore = new PropertiesFileAttributeStore(file.getAbsolutePath());
            attributeStore.get("foo");
            attributeStore.set("foo", "bar");
            attributeStore.set("foo", "bar");
        } finally {
            boolean deleted = deleteDir(dir);
            if (!deleted) {
                //noinspection ThrowFromFinallyBlock
                throw new IllegalStateException("Failed to delete directory " + dir.getAbsolutePath());
            }
        }
    }

    @Test
    public void testRemove() throws Exception {
        attributeStore1.set("foo", "bar");
        assertThat(attributeStore1.get("foo"), is("bar"));
        assertThat(attributeStore1.remove("foo"), is("bar"));
        assertThat(attributeStore1.get("foo"), is(nullValue()));
        assertThat(attributeStore1.remove("foo"), is(nullValue()));
    }

    @Test
    public void testWithProperties() throws Exception {
        PropertiesHandler<String> propertiesHandler = new PropertiesHandler<String>() {
            @Override
            public String handle(Properties properties) {
                properties.setProperty("foo", "bar");
                return properties.getProperty("foo");
            }
        };
        assertThat(attributeStore1.withProperties(propertiesHandler), is("bar"));
        assertThat(attributeStore1.get("foo"), is("bar"));
    }


    private boolean deleteDir(File dir) {
        //noinspection ResultOfMethodCallIgnored
        dir.listFiles(new FileFilter() {
            @Override
            public boolean accept(File pathname) {
                //noinspection ResultOfMethodCallIgnored
                pathname.delete();
                return false;
            }
        });
        return dir.delete();
    }

}

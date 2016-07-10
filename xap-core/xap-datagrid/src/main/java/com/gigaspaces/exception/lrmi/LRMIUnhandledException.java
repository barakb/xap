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

package com.gigaspaces.exception.lrmi;

/**
 * A special exception which is not handled by the LRMI layer and its cause will be thrown to the
 * client
 *
 * @author eitany
 * @since 7.5
 */
@com.gigaspaces.api.InternalApi
public class LRMIUnhandledException
        extends RuntimeException {
    private static final long serialVersionUID = 1L;
    private final Stage _stage;

    public enum Stage {
        DESERIALIZATION
    }

    public LRMIUnhandledException(Stage stage) {
        super();
        _stage = stage;
    }

    public LRMIUnhandledException(Stage stage, String msg) {
        super(msg);
        _stage = stage;
    }

    public LRMIUnhandledException(Stage stage, String msg, Throwable cause) {
        super(msg, cause);
        _stage = stage;
    }

    public Stage getStage() {
        return _stage;
    }
}

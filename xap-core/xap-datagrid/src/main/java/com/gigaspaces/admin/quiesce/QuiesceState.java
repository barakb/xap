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


package com.gigaspaces.admin.quiesce;

/**
 * @author Boris
 * @since 10.1.0
 */
@SuppressWarnings("SpellCheckingInspection")
public enum QuiesceState {
    QUIESCED(0),
    UNQUIESCED(1);

    private final int code;

    private QuiesceState(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }

    public static QuiesceState traslateCodeToState(int code) {
        switch (code) {
            case 0:
                return QuiesceState.QUIESCED;
            case 1:
                return QuiesceState.UNQUIESCED;
            default:
                throw new UnsupportedOperationException("No such code: " + code + " for QuiesceState");
        }
    }
}

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

import com.gigaspaces.annotation.pojo.SpaceId;

public class Phrase {
    private String englishPhrase;
    private String spanishPhrase;

    public Phrase() {
    }

    public Phrase(String englishPhrase) {
        this.englishPhrase = englishPhrase;
    }

    @SpaceId(autoGenerate = false)
    public String getEnglishPhrase() {
        return englishPhrase;
    }

    public void setEnglishPhrase(String englishPhrase) {
        this.englishPhrase = englishPhrase;
    }

    public String getSpanishPhrase() {
        return spanishPhrase;
    }

    public void setSpanishPhrase(String spanishPhrase) {
        this.spanishPhrase = spanishPhrase;
    }

    @Override
    public String toString() {
        if (spanishPhrase == null) {
            return "\'" + englishPhrase + "\'";
        } else {
            return "\'" + spanishPhrase + "\'";
        }
    }
}

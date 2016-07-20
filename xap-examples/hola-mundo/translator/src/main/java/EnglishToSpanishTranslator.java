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
import com.j_spaces.core.client.SQLQuery;

import org.openspaces.events.EventDriven;
import org.openspaces.events.EventTemplate;
import org.openspaces.events.adapter.SpaceDataEvent;
import org.openspaces.events.polling.Polling;

import java.util.HashMap;
import java.util.Map;

@EventDriven
@Polling
public class EnglishToSpanishTranslator {

    private static Map<String, String> dictionary = new HashMap<String, String>();

    public EnglishToSpanishTranslator() {
        dictionary.put("Hello", "Hola");
        dictionary.put("World!", "Mundo!");
    }

    @EventTemplate
    SQLQuery<Phrase> untranslated() {
        return new SQLQuery<Phrase>(Phrase.class, "spanishPhrase is null");
    }

    @SpaceDataEvent
    public Phrase translate(Phrase phrase) {
        String englishPhrase = phrase.getEnglishPhrase();
        String englishToSpanishTranslation = dictionary.get(englishPhrase);
        if (englishToSpanishTranslation != null) {
            phrase.setSpanishPhrase(englishToSpanishTranslation);
        } else {
            phrase.setSpanishPhrase("unknown <-> desconocido");
        }

        System.out.println("translated - " + phrase.getEnglishPhrase() + " to " + phrase.getSpanishPhrase());
        return phrase;
    }
}

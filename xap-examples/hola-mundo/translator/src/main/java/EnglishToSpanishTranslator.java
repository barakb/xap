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
    SQLQuery<Phrase> untranslatedMessage() {
        return new SQLQuery<Phrase>(Phrase.class, "spanishPhrase is null");
    }

    @SpaceDataEvent
    public Phrase translate(Phrase phrase) {
        String englishPhrase = phrase.getEnglishPhrase();
        String englishToSpanishTranslation = dictionary.get(englishPhrase);
        if (englishToSpanishTranslation != null) {
            phrase.setSpanishPhrase(englishToSpanishTranslation);
        } else {
            phrase.setSpanishPhrase("unkown <-> desconocido");
        }

        System.out.println("translated - " + phrase.getEnglishPhrase() + " to " + phrase.getSpanishPhrase());
        return phrase;
    }
}

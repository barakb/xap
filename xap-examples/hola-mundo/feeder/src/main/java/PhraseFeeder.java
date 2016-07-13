import com.j_spaces.core.LeaseContext;

import org.openspaces.core.GigaSpace;
import org.openspaces.core.context.GigaSpaceContext;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;

import java.util.Arrays;


public class PhraseFeeder implements InitializingBean, DisposableBean {


    @GigaSpaceContext
    private GigaSpace space;

    public void afterPropertiesSet() throws Exception {

        write(space, new Phrase("Hello"));
        write(space, new Phrase("World!"));

        read(space, new Phrase());

    }

    public void destroy() throws Exception {

    }

    /**
     * Write (or update) an entity in the data-grid
     */
    private static void write(GigaSpace space, Phrase phrase) {
        LeaseContext<Phrase> context = space.write(phrase);

        if (context.getVersion() == 1) {
            System.out.println("write - " + phrase);
        } else {
            System.out.println("update - " + phrase);
        }
    }

    /**
     * Read a matching entity from the data-grid Template matching is done by field equality or any
     * if field is null
     */
    private static void read(GigaSpace space, Phrase template) {
        Phrase[] results = space.readMultiple(template);
        System.out.println("read - " + Arrays.toString(results));
    }

}

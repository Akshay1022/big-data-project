package scraper;

import java.net.URL;
import java.util.Comparator;

/**
 * Data structure for storing a URL in a queue with a given prioirty.
 * */

public class PrioritizedURL {

    public URL url;
    public Integer priority;

    public PrioritizedURL(URL url, Integer priority){
        this.url = url;
        this.priority = priority;
    }

    public static PrioritizedURLComparator comparator() {
        return new PrioritizedURLComparator();
    }
}

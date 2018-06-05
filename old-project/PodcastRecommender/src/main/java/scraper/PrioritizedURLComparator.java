package scraper;

import java.util.Comparator;

/**
 * Convenience comparator for sorting PrioritizedURLs.
 */

public class PrioritizedURLComparator implements Comparator<PrioritizedURL> {
    @Override
    public int compare(PrioritizedURL o1, PrioritizedURL o2) {
        PrioritizedURL first = (PrioritizedURL)o1;
        PrioritizedURL second = (PrioritizedURL)o2;
        return first.priority.compareTo(second.priority);
    }
}
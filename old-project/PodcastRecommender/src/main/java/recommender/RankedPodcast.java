package recommender;

import model.Podcast;

/**
 * Datastructure that holds a reference to a Podcast object along with a
 * rank. It is designed for use with a PriorityQueue.
 */
public class RankedPodcast implements Comparable<RankedPodcast> {

    private Podcast podcast;
    private Double rank;

    public RankedPodcast(Podcast p, Double r){
        podcast = p;
        rank = r;
    }

    public Podcast getPodcast() {
        return podcast;
    }

    public void setPodcast(Podcast podcast) {
        this.podcast = podcast;
    }

    public Double getRank() {
        return rank;
    }

    public void setRank(Double rank) {
        this.rank = rank;
    }

    /**
     * Comparison returns the greater rank as lesser than to accomodate the way
     * PriorityQueue ranks items.
     * @param o
     * @return
     */
    @Override
    public int compareTo(RankedPodcast o) {
        if (this.rank == o.rank) { return 0;}
        if (this.rank > o.rank) { return -1;}
        return 1;
    }
}

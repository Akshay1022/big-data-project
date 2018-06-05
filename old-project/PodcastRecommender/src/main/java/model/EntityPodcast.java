package model;

/**
 * Data structure that represents database row NAMEDENTITY_PODCAST.
 */
public class EntityPodcast {

    private Podcast podcast;
    private NamedEntity entity;
    private Integer weight;

    public Podcast getPodcast() {
        return podcast;
    }

    public void setPodcast(Podcast podcast) {
        this.podcast = podcast;
    }

    public NamedEntity getEntity() {
        return entity;
    }

    public void setEntity(NamedEntity entity) {
        this.entity = entity;
    }

    public Integer getWeight() {
        return weight;
    }

    public void setWeight(Integer weight) {
        this.weight = weight;
    }
}

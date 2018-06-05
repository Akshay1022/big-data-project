package model;

/**
 * Data structure that represents the database table KEYWORD_PODCAST rows.
 */
public class KeywordPodcast {

    private Podcast podcast;
    private Keyword keyword;
    private Integer weight;

    public Podcast getPodcast() {
        return podcast;
    }

    public void setPodcast(Podcast podcast) {
        this.podcast = podcast;
    }

    public Keyword getKeyword() {
        return keyword;
    }

    public void setKeyword(Keyword keyword) {
        this.keyword = keyword;
    }

    public Integer getWeight() {
        return weight;
    }

    public void setWeight(Integer weight) {
        this.weight = weight;
    }
}

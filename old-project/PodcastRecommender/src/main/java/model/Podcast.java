package model;

import java.util.ArrayList;
import java.util.List;

/**
 * Data structure that represents the database table PODCAST rows.
 */
public class Podcast {

    private Integer id;
    private Integer itunesId;
    private String itunes_url;
    private String title;
    private String feedUrl;
    private String category;
    private String description;
    private String artworkUrl;
    private String authors;
    private String rawText;

    private List<Keyword> keywords = new ArrayList<>();
    private List<NamedEntity> namedEntities = new ArrayList<>();


    /******** GETTERS AND SETTERS *******/

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public Integer getItunesId() {
        return itunesId;
    }

    public void setItunesId(Integer itunesId) {
        this.itunesId = itunesId;
    }

    public String getItunes_url() {
        return itunes_url;
    }

    public void setItunes_url(String itunes_url) {
        this.itunes_url = itunes_url;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getFeedUrl() {
        return feedUrl;
    }

    public void setFeedUrl(String feedUrl) {
        this.feedUrl = feedUrl;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getArtworkUrl() {
        return artworkUrl;
    }

    public void setArtworkUrl(String artworkUrl) {
        this.artworkUrl = artworkUrl;
    }

    public List<Keyword> getKeywords() {
        return keywords;
    }

    public void setKeywords(List<Keyword> keywords) {
        this.keywords = keywords;
    }

    public List<NamedEntity> getNamedEntities() {
        return namedEntities;
    }

    public void setNamedEntities(List<NamedEntity> namedEntities) {
        this.namedEntities = namedEntities;
    }

    public String getAuthors() {
        return authors;
    }

    public void setAuthors(String authors) {
        this.authors = authors;
    }

    public String getRawText() {
        return rawText;
    }

    public void setRawText(String rawText) {
        this.rawText = rawText;
    }
}

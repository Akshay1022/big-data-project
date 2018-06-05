package edu.utdallas.spark;

import java.util.ArrayList;
import java.util.List;

public class Podcast {

    private Integer itunesId;
    private String itunes_url;
    private String title;
    private String feedUrl;
    private String category;
    private String description;
    private List<String> authors = new ArrayList<String>();

    private List<Episode> episodes = new ArrayList<Episode>();
    private List<Keyword> keywords = new ArrayList<Keyword>();


    /******** GETTERS AND SETTERS *******/

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

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public List<Keyword> getKeywords() {
        return keywords;
    }

    public void setKeywords(List<Keyword> keywords) {
        this.keywords = keywords;
    }

    public List<String> getAuthors() {
        return authors;
    }

    public void setAuthors(List<String> authors) {
        this.authors = authors;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public List<Episode> getEpisodes() {
        return episodes;
    }

    public void setEpisodes(List<Episode> episodes) {
        this.episodes = episodes;
    }
}

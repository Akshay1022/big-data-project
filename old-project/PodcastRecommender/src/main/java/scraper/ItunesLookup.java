package scraper;

import analyzer.TextAnalyzer;
//import clover.com.google.common.collect.Lists;
//import clover.org.apache.velocity.util.ArrayListWrapper;
import model.*;
import data.DB;
import org.json.JSONArray;
import org.json.JSONObject;
import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.parser.Parser;
import org.jsoup.select.Elements;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Class than can be used to load iTunes podcast IDs from the database, then
 * retrieve and parse the JSON metadata from the iTunes store API, then fetch the
 * podcast feed XML, parse it, and use TextAnalyzer to extract the podcast information,
 * keywords and named entities from it.
 *
 */
public class ItunesLookup {

    static BlockingQueue<String> lookupIds = new LinkedBlockingDeque<>();
    static BlockingQueue<Podcast> processedPodcasts = new LinkedBlockingDeque<>();
    static BlockingQueue<Feed> lookedUpFeeds = new LinkedBlockingDeque<>();

    static ConcurrentHashMap<String, Keyword> allKeywords = new ConcurrentHashMap<>();
    static ConcurrentHashMap<String, NamedEntity> allEntities = new ConcurrentHashMap<>();

    static BlockingQueue<KeywordPodcast> keywordPodcasts = new LinkedBlockingDeque<>();
    static BlockingQueue<EntityPodcast> entityPodcasts = new LinkedBlockingDeque<>();

    static int threadCount = 0;
    static int MAX_THREAD_COUNT = 2;

    static final String BASE_URL = "https://itunes.apple.com/lookup?id=";

    public static JSONObject lookup(String itunesId) {
        Document doc = null;
        try {
            Connection connection = Jsoup.connect(BASE_URL + itunesId);
            doc = connection.get();
            JSONObject json = new JSONObject(doc.select("body").html().toString());
            JSONArray results = (JSONArray) json.get("results");
            JSONObject podcastJson = results.getJSONObject(0);
            return podcastJson;
        } catch (Exception e) {
            return null;
        }
    }

    private static void addPeriodToListItem(Element parentListItem) {
        parentListItem.html(". " + parentListItem.html() + ". ");
        List<Element> listItems = parentListItem.children().select("li");

        for (Element listItem : listItems) {
            addPeriodToListItem(listItem);
        }
    }

    private static void addKeywordStringsToAllKeywords(List<String> keywords, Podcast podcast) {
        List<Keyword> list = new ArrayList();
        for (String keyword : keywords) {
            Keyword e = new Keyword();
            e.setWord(keyword);
            e.setWeight(10);
            list.add(e);
        }

        addKeywordsToAllKeywords(list, podcast);
    }

    private static void addEntityStringsToAllEntities(List<String> entities, Podcast podcast) {
        List<NamedEntity> list = new ArrayList();
        for (String entity : entities) {
            NamedEntity e = new NamedEntity();
            e.setName(entity);
            e.setWeight(10);
            list.add(e);
        }

        addEntitiesToAllEntities(list, podcast);
    }

    private static void addKeywordsToAllKeywords(List<Keyword> keywords, Podcast podcast) {
        for (Keyword keyword : keywords) {
            Integer weight = keyword.getWeight();

            Keyword k;
            if (allKeywords.containsKey(keyword.getWord())) {
                k = allKeywords.get(keyword.getWord());
            } else {
                k = keyword;
                allKeywords.put(k.getWord(), k);
            }

            podcast.getKeywords().add(k);

            KeywordPodcast kp = new KeywordPodcast();
            kp.setWeight(weight);
            kp.setKeyword(k);
            kp.setPodcast(podcast);

            keywordPodcasts.add(kp);
        }
    }

    private static void addEntitiesToAllEntities(List<NamedEntity> entities, Podcast podcast) {
        for (NamedEntity entity : entities) {
            Integer weight = entity.getWeight();

            NamedEntity e;
            if (allEntities.containsKey(entity.getName())) {
                e = allEntities.get(entity.getName());
            } else {
                e = entity;
                allEntities.put(e.getName(), e);
            }

            podcast.getNamedEntities().add(e);

            EntityPodcast ep = new EntityPodcast();
            ep.setWeight(weight);
            ep.setEntity(e);
            ep.setPodcast(podcast);

            entityPodcasts.add(ep);
        }
    }

    public static Podcast processFeed(String feedUrl) throws java.io.IOException {
        Podcast podcast = new Podcast();

        Connection connection = Jsoup.connect(feedUrl);

        Document xml;
        try {
            xml = connection.get();
        } catch (Exception e) {
            return null;
        }

        String title, authors, description, category;
        try {
            title = xml.getElementsByTag("title").first().html();
            authors = xml.getElementsByTag("itunes:author").first().html();
            description = xml.getElementsByTag("description").first().html();
            category = xml.getElementsByTag("itunes:category").attr("text");
        } catch (Exception e) {
            return null;
        }

        //System.out.println("processing feed"+ feedUrl);

        podcast.setTitle(title);
        podcast.setDescription(description);
        podcast.setFeedUrl(feedUrl);
        podcast.setCategory(category);

        String text = title + ". " + authors + ". " + description + ". ";

        Elements keywordElements = xml.getElementsByTag("itunes:keywords");
        if (keywordElements != null && keywordElements.size() > 0) {
//            for (String keyword : keywordElements.first().html().toString().split(",")) {
//                Keyword k = new Keyword();
//                k.setWord(keyword);
//                k.setWeight(10);
//                podcast.getKeywords().add(k);
//            }
            String[] keywords = keywordElements.first().html().toString().split(",");


//            addKeywordStringsToAllKeywords(Lists.newArrayList(keywords), podcast);


        }



        // TODO: add title, authors, podcast description, itunes:keywords, itunes:category


        for (Element item : xml.getElementsByTag("item")) {

            // TODO: add itunes:keywords for each episode
            Elements episodeKeywordElements = xml.getElementsByTag("itunes:keywords");
            if (episodeKeywordElements != null && episodeKeywordElements.size() > 0) {
//                for (String keyword : episodeKeywordElements.first().html().toString().split(",")) {
//                    Keyword k = new Keyword();
//                    k.setWord(keyword);
//                    podcast.getKeywords().add(k);
//                }

                String[] keywords = episodeKeywordElements.first().html().toString().split(",");


//                addKeywordStringsToAllKeywords(Lists.newArrayList(keywords), podcast);



            }

            // convert encoded html of each episode's description into an html documen

            Elements descriptions = item.getElementsByTag("description");
            if (descriptions != null && descriptions.size() > 0) {
                Document descriptionHtml = Jsoup.parse(Parser.unescapeEntities(descriptions.first().html(), true));

                List<Element> listElements = descriptionHtml.select("li");

                for (Element listElement : listElements) {
                    addPeriodToListItem(listElement);
                }

                text += descriptionHtml.text() + ". ";

            }
        }

        text.replaceAll("\\.", ". .");

        //System.out.println("\trunning text analyzer");
        TextAnalyzer analyzer = new TextAnalyzer();
        analyzer.analyze(text);

        //System.out.println("\tadding keywords");
        addKeywordsToAllKeywords(analyzer.getKeywords(), podcast);
        addEntitiesToAllEntities(analyzer.getNamedEntities(), podcast);

        //System.out.println("\tdone processing feed"+feedUrl);
        return podcast;
    }


    public static void main(String args[]) {


        try {

            List<String> dirtyIds = DB.getInstance().getItunesIds();
            List<String> processedItunesIds = DB.getInstance().getPodcastItunesIds();


            for (Keyword k : DB.getInstance().fetchKeywords()) {
                allKeywords.put(k.getWord(), k);
            }

            for (NamedEntity e : DB.getInstance().fetchNamedEntities()) {
                allEntities.put(e.getName(), e);
            }

            int i = 0;
            int limit = 15000;
            for (String id : dirtyIds) {
                int index = id.indexOf("/id");
                if (index >= 0) {
                    id = id.substring(index + 3);
                }

                boolean alreadyProcessed = false;

                for (String processedId : processedItunesIds) {
                    if (processedId.compareTo(id) == 0) {
                        alreadyProcessed = true;
                        break;
                    }
                }
                if (!alreadyProcessed) {
                    lookupIds.add(id);
                    i++;
                }
                if (i >= limit) {
                    break;
                }
            }

            final int count = lookupIds.size();

            while (!lookupIds.isEmpty()) {
                if (threadCount < MAX_THREAD_COUNT) {
                    int numIds = lookupIds.size();
                    if (numIds % 100 == 0) {
                        System.out.println("ids left: " +numIds);
                    }
                    final String lookupId = lookupIds.remove();


                    new Thread(new Runnable() {
                        public void run() {
                            threadCount++;
                            JSONObject json = lookup(lookupId);
                            if (json != null) {
                                String feedUrl = json.getString("feedUrl");

                                if (feedUrl != null) {
                                    Feed feed = new Feed();
                                    feed.url = feedUrl;
                                    feed.itunesId = lookupId;

                                    try {
                                        Podcast podcast = processFeed(feed.url);
                                        if (podcast != null) {
                                            podcast.setItunesId(Integer.parseInt(feed.itunesId));
                                            processedPodcasts.add(podcast);
                                            System.out.println("processed feed ("+processedPodcasts.size()+"): " + feedUrl);
                                        }

                                    } catch (Exception e) {
                                        e.printStackTrace();
                                    }
                                }
                            }
                            threadCount--;
                        }
                    }).start();
                }
            }

            if (threadCount != 0) {
                System.out.println("sleep");
                Thread.sleep(1000);
            }

            System.out.println("SAVING DATA TO DATABASE...");

            System.out.println(processedPodcasts);
            System.out.println(allKeywords);
            System.out.println(allEntities);

            DB db = DB.getInstance();

            System.out.println("\tSaving podcasts.");
            db.batchSavePodcasts(processedPodcasts);

            System.out.println("\tSaving keywords");
            db.batchSaveKeywords(allKeywords);

            System.out.println("\tSaving entities");
            db.batchSaveEntities(allEntities);

            System.out.println("\tSaving keywordPodcasts");
            db.batchSaveKeywordPodcasts(keywordPodcasts);

            System.out.println("\tSaving entityPodcasts");
            db.batchSaveEntityPodcasts(entityPodcasts);

        } catch (Exception e) {
            System.out.println(e.getClass() + ": " + e.getMessage());
            e.printStackTrace();
        }
    }
}

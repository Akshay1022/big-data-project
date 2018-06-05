package scraper;

//import analyzer.TextAnalyzer;
//import clover.com.google.common.collect.Lists;
import data.DB;
import model.*;
import org.json.JSONArray;
import org.json.JSONObject;
import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.parser.Parser;
import org.jsoup.select.Elements;

import java.util.ArrayList;
import java.util.List;
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
public class ItunesFeedUrlLookup {

    static BlockingQueue<String> lookupIds = new LinkedBlockingDeque<>();
    static BlockingQueue<Podcast> processedPodcasts = new LinkedBlockingDeque<>();
    static BlockingQueue<Feed> lookedUpFeeds = new LinkedBlockingDeque<>();

    static int threadCount = 0;
    static int MAX_THREAD_COUNT = 3;

    static final String BASE_URL = "https://itunes.apple.com/lookup?id=";

    public static JSONObject lookup(String itunesId) {
        Document doc = null;
        JSONArray results = null;
        JSONObject json = null;
        JSONObject podcastJson = null;

        boolean isFetchError = false;

        do {

            try {
                Connection connection = Jsoup.connect(BASE_URL + itunesId);
                doc = connection.get();
                json = new JSONObject(doc.select("body").html().toString());
                results = (JSONArray) json.get("results");
                podcastJson = results.getJSONObject(0);
                return podcastJson;
            } catch (Exception e) {
                System.out.println(e.getLocalizedMessage() + "("+itunesId+")");
                if (e.getLocalizedMessage().compareTo("HTTP error fetching URL") == 0 ||
                        e.getLocalizedMessage().compareTo("Read timed out") == 0)
                {
                    //System.out.println(e.getLocalizedMessage() + " ("+itunesId+") - trying again after a nap...");
                    try {
                        Thread.sleep(1000);
                    }
                    catch (Exception ex){

                    }
                    isFetchError = true;
                }
                else {
                    isFetchError = false;
                }
                //            System.out.println("itunes id: " + itunesId);
                //            System.out.println(results);
                //            System.out.println(json);
                //            System.out.println(doc);
                //return null;
            }
        } while (isFetchError);

        return podcastJson;
    }


    public static void main(String args[]) {


        try {

            //List<String> dirtyIds = DB.getInstance().getItunesIds();
            List<String> dirtyIds = DB.getInstance().getNewItunesIds(); // Get Id's added added in 2017
            List<String> processedItunesIds = DB.getInstance().getFeedUrlItunesIds();

            System.out.println("Preparing ids...");

            int i = 0;
            int limit = 252409;
//            int limit = 50000;
            int j = 0;

            // Some id's in DB were not scraped properly and must be cleaned up
            for (String id : dirtyIds) {
                j++;
                int index = id.indexOf("/id");
                if (index >= 0) {
                    id = id.substring(index + 3);
                }

                boolean alreadyProcessed = false;

                // Check if this id has already been processed
                for (String processedId : processedItunesIds) {
                    if (processedId.compareTo(id) == 0) {
                        alreadyProcessed = true;
                        break;
                    }
                }
                if (i % 100 == 0) {
                    System.out.println("\tPrepared " + j + " ids.");
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

            System.out.println("Starting lookup of " + count + "ids");

            while (!lookupIds.isEmpty()) {
                if (threadCount < MAX_THREAD_COUNT) {
                    int numIds = lookupIds.size();
                    if (numIds % 100 == 0) {
                        System.out.println("ids left: " +numIds);
                    }
                    else {
                        System.out.println("\t" +numIds);
                    }
                    final String lookupId = lookupIds.remove();


                    new Thread(new Runnable() {
                        public void run() {
                            threadCount++;
                            JSONObject json = lookup(lookupId);

                            if (json == null){
                                Podcast podcast = new Podcast();
                                podcast.setItunesId(Integer.parseInt(lookupId));
                               // processedPodcasts.add(podcast);

                                DB db = DB.getInstance();
                                db.savePodcastToFeedUrl(podcast);
                            }

                            if (json != null) {
                                String feedUrl = json.getString("feedUrl");

                                if (feedUrl != null) {
                                    Feed feed = new Feed();
                                    feed.url = feedUrl;
                                    feed.itunesId = lookupId;

                                    Podcast podcast = new Podcast();
                                    podcast.setItunesId(Integer.parseInt(lookupId));
                                    podcast.setFeedUrl(feedUrl);

                                    DB db = DB.getInstance();
                                    db.savePodcastToFeedUrl(podcast);
                                }
                            }
                            threadCount--;
                        }
                    }).start();
                }
            }

            if (threadCount != 0) {
                System.out.println("Ending...");
                Thread.sleep(1000);
            }


        } catch (Exception e) {
            System.out.println(e.getClass() + ": " + e.getMessage());
            e.printStackTrace();
        }
    }
}

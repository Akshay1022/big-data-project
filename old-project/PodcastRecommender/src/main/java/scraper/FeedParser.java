package scraper;

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

//import clover.com.google.common.collect.Lists;
//import clover.org.apache.velocity.util.ArrayListWrapper;

/**
 * Class than can be used to load feed urls from the database, then fetch the
 * podcast feed XML, parse it and save podcast metadata and episode info to the database.
 *
 */
public class FeedParser {


    static BlockingQueue<Feed> lookedUpFeeds = new LinkedBlockingDeque<>();
    static BlockingQueue<Feed> lookupFeeds = new LinkedBlockingDeque<>();

    static int threadCount = 0;
    static int MAX_THREAD_COUNT = 32;

    static final String BASE_URL = "https://itunes.apple.com/lookup?id=";


    // Turns <li> list items into sententeces with a period at the end to aid in keyword analysis.
    private static void addPeriodToListItem(Element parentListItem) {
        parentListItem.html(". " + parentListItem.html() + ". ");
        List<Element> listItems = parentListItem.children().select("li");

        for (Element listItem : listItems) {
            addPeriodToListItem(listItem);
        }
    }

    public static Podcast processFeed(String feedUrl) throws java.io.IOException {
        Podcast podcast = new Podcast();
        Connection connection = null;

        try {
            connection = Jsoup.connect(feedUrl);
        }
        catch (Exception e){
            System.out.println("Unable to get feed: " + feedUrl);
            e.printStackTrace();
            return null;
        }

        Document xml;
        try {
            xml = connection.get();
        } catch (Exception e) {
            System.out.println("Unable to get XML: " + feedUrl);
            return null;
        }

        // Get and set Podcast metadata
        String title, authors, description, category;
        try {
            title = xml.getElementsByTag("title").first().html();
            authors = xml.getElementsByTag("itunes:author").first().html();
            description = xml.getElementsByTag("description").first().html();
            category = xml.getElementsByTag("itunes:category").attr("text");
        } catch (Exception e) {
            System.out.println("Unable to get metadata: " + feedUrl);
            e.printStackTrace();
            return null;
        }

        podcast.setTitle(title);
        podcast.setDescription(description);
        podcast.setFeedUrl(feedUrl);
        podcast.setCategory(category);
        podcast.setAuthors(authors);

        String text = "";

        // Get Episode information.
        for (Element item : xml.getElementsByTag("item")) {
            String episodeTitle = item.getElementsByTag("title").first().text();
            text += episodeTitle + ":: ";

            Elements descriptions = item.getElementsByTag("description");
            if (descriptions != null && descriptions.size() > 0) {
                Document descriptionHtml = Jsoup.parse(Parser.unescapeEntities(descriptions.first().html(), true));

                List<Element> listElements = descriptionHtml.select("li");

                for (Element listElement : listElements) {
                    addPeriodToListItem(listElement);
                }

                text += descriptionHtml.text() + ". ";
            }

            // seperator for the end of episode text
            text += "::::";
        }

        if (text.contains("::::")) {
            text.substring(0, text.length() - 4);
        }
        text.replaceAll("\\.", ". .");

        podcast.setRawText(text);
        return podcast;
    }


    public static void main(String args[]) {

        try {

            // Get feeds from DB
            // TODO: replace DB read with read from feeds file.
            List<Feed> feedUrls = DB.getInstance().getFeeds(1200); // limit on number of feeds processed
            lookupFeeds.addAll(feedUrls);

            // Multithreaded to avoid waiting for network response.
            while (!lookupFeeds.isEmpty()) {
                if (threadCount < MAX_THREAD_COUNT) {
                    int numIds = feedUrls.size();
                    if (numIds % 100 == 0) {
                        System.out.println("feeds left: " +numIds);
                    }
                    final Feed feed = lookupFeeds.remove();

                    // Create new thread to look up feed, parse, and save to DB.
                    new Thread(new Runnable() {
                        public void run() {
                            threadCount++;
                            System.out.println("Looking up feed: " + feed.url);
                            try {
                                Podcast podcast = processFeed(feed.url);

                                // If podcast is null then there was a network or parse error.
                                // A percentatge of feed urls do not return useful results.
                                if (podcast != null) {
                                    podcast.setItunesId(Integer.parseInt(feed.itunesId));

                                    // TODO: replace save to DB with
                                    DB.getInstance().savePodcastToPodcast2(podcast);
                                }

                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                            System.out.println("Finished feed: " + feed.url);
                            threadCount--;
                        }
                    }).start();
                }
            }

            // Give threads a chance to finish work
            if (threadCount != 0) {
                System.out.println("sleep");
                Thread.sleep(1000);
            }

        } catch (Exception e) {
            System.out.println(e.getClass() + ": " + e.getMessage());
            e.printStackTrace();
        }
    }
}

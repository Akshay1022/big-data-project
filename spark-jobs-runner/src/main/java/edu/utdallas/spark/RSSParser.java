package edu.utdallas.spark;
import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.parser.Parser;
import org.jsoup.select.Elements;

import java.util.List;

public class RSSParser {

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

        if (authors != null) {
            for (String author : authors.split(",")){
                podcast.getAuthors().add(author.trim());
            }
        }

        podcast.setTitle(title);
        podcast.setDescription(description);
        podcast.setFeedUrl(feedUrl);
        podcast.setCategory(category);

        try {
            // Get Episode information.
            for (Element item : xml.getElementsByTag("item")) {

                Episode episode = new Episode();
                episode.setTitle(item.getElementsByTag("title").first().text());

                Elements descriptions = item.getElementsByTag("description");
                if (descriptions != null && descriptions.size() > 0) {
                    Document descriptionHtml = Jsoup.parse(Parser.unescapeEntities(descriptions.first().html(), true));

                    List<Element> listElements = descriptionHtml.select("li");

                    for (Element listElement : listElements) {
                        addPeriodToListItem(listElement);
                    }

                    episode.setDescription(descriptionHtml.text());
                }

                podcast.getEpisodes().add(episode);
            }
        }
        catch (Exception e){
            System.out.println("Unable to get episodes: ");
            e.printStackTrace();
        }

        return podcast;
    }

}

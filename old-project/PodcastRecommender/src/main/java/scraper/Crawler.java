package scraper;

import com.sun.xml.internal.ws.util.StringUtils;
import data.DB;
import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;
import java.util.concurrent.PriorityBlockingQueue;

/**
* DEFUNCT CLASS. REPLACED BY DOMAIN-SPECIFIC ITUNESCRAWLER
 */
public class Crawler {

    PriorityBlockingQueue<PrioritizedURL> links;
    HashMap<String, Boolean> processed = new HashMap<>();

    ArrayList<String> keywords = new ArrayList<>();

    int MAX_THREADS = 32;
    int threads = 0;

    int MAX_HISTORY = 1000000;
    int MAX_QUEUE = 10000;

    private boolean locked = false;

    public Crawler(){
        keywords.add("podcast");
        keywords.add("subscribe");
        keywords.add("rss");
        keywords.add("feed");
        keywords.add("itunes.apple.com");
        keywords.add("xml");
        keywords.add("/podcast/");
        keywords.add("genre/podcasts");

        links = new PriorityBlockingQueue<>(MAX_QUEUE, PrioritizedURL.comparator());
    }

    public void crawl(URL seed){
        links.clear();
        links.add(new PrioritizedURL(seed, 0));

        while (true) {
            if (!links.isEmpty() && threads < MAX_THREADS) {
                consume();
            }
        }
    }

    public void consume(){
       // System.out.print(".");
        URL link = null;
        try {
            PrioritizedURL purl = links.take();
            link = purl.url;
        }
        catch (InterruptedException e) {
            return;
        }
        if (processed.size() > MAX_HISTORY){
            processed.clear();
        }
        if (!processed.containsKey(link.toString())) {
            processed.put(link.toString(), true);
            process(link);
        } else {
            //System.out.println("Already Processed: " + link);
        }
    }

    public void process(URL link){
        //if (threads < MAX_THREADS) {
            threads++;
            new Thread(new Runnable() {
                public void run() {
                    Document doc = null;
                    try {
                        Connection connection = Jsoup.connect(link.toString());
                        doc = connection.get();
                    } catch (Exception e) {
                        doc = null;
                    }
                    analyze(link, doc);
                    threads--;
                }
            }).start();
       // }
    }

    public void analyze(URL link, Document doc){
        if (doc != null) {
            if (doc.select("rss[xmlns:itunes]").size() > 0
                    || doc.select("feed[xmlns:itunes]").size() > 0){
                System.out.println("\tFEED: " + link);
                DB.insertFeedUrl(link.toString());
            }
            else if (doc.select("html").size() > 0) {
                System.out.println(link);

                Elements ahrefs = doc.select("a[href]");
                for (Element a : ahrefs){
                    URL url = null;
                    try {
                        url = new URL(a.attr("href"));
                    }
                    catch (MalformedURLException e){
                        //System.out.println(e.getLocalizedMessage());
                    }
                    if (url != null) {
                        int priority = keywords.size() * 3;
                        String urlString = url.toString().toLowerCase();
                        for (String keyword : keywords){
                            if (link.toString().contains(keyword)){
                                priority -= 2;
                            }
                            if (urlString.indexOf(keyword) >= 0) {
//                                if (keyword == "feed" || keyword == "rss" || keyword == "itunes"){
//                                    priority = 0;
//                                    break;
//                                }
//                                else {
//                                    priority = 1;
//                                    break;
//                                }
                                priority--;
                            }
                        }
                        //System.out.println("\tpriority: " + priority + ", url: " + url);
                        links.put(new PrioritizedURL(url, priority));
                    }
                }
            }
        }
    }

    public static void main(String[] args){
//        String seed = "http://atp.fm/episodes?format=rss";
//        String seed2 = "https://ricochet.com/series/ricochet-podcast/feed/";
//        String seed3 = "http://blog.shiftyjelly.com/2009/06/07/our-first-third-party-application/feed/";
//        Document html = null;
//        try {
//            html = Jsoup.connect(seed3).get();
//        }
//        catch (IOException e){
//            System.out.println(e.getLocalizedMessage());
//        }
//        if (html != null){
//            if (html.select("rss").size() > 0){
//                System.out.println(html);
//                return;
//            }
//            System.out.println(html);
//            Elements links = html.select("a[href]");
//            for (Element e : links){
//                System.out.println(e.attr("href"));
//            }
//        }

        Crawler crawler = new Crawler();
        try {
            crawler.crawl(new URL("https://itunes.apple.com/us/genre/podcasts/id26?mt=2"));
        }
        catch (MalformedURLException e) {
            //System.out.println(e.getLocalizedMessage());
        }
    }
}

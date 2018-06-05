package recommender;

import data.DB;
import model.Keyword;
import model.Podcast;
import model.Word;

import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;

/**
 * Class that ranks and matches Podcasts with a set of IDs representing a user's subscription.
 * The main method provides the example for the usage.
 */
public class Recommender {

    public static PriorityBlockingQueue<RankedPodcast> ranked = new PriorityBlockingQueue<>();
    public static LinkedBlockingQueue<Podcast> podcastQueue = new LinkedBlockingQueue<>();
    public static int threads = 0;
    public static int MAX_THREAD_COUNT = 3;

    // Normalize tf score.
    public Map<Integer, Double> tf(List<Word> words){

        Map<Integer, Double> tfScoreForWordId = new HashMap<>();

        double numberOfWords = 0.0;
        for (Word w : words){
            numberOfWords = w.getWeight();
        }

        for (Word w : words){
            tfScoreForWordId.put(w.getId(), w.getWeight() / numberOfWords);
        }

        return tfScoreForWordId;
    }


    // idf(term) = log( NumDocs / df(term) )
    public Map<Integer, Double> calculateIdf(List<? extends Word> words, Integer numberOfPodcasts){
        Map<Integer, Double> map = new HashMap<>();
        for (Word word : words){
            Double idf = Math.log(numberOfPodcasts/word.getDf());
            map.put(word.getId(), idf);
        }
        return map;
    }

    public Map<Integer, Integer> compositeKeywordWeights(List<Podcast> podcasts){

        Map<Integer, Integer> weights = new HashMap<>();

        for (Podcast podcast : podcasts) {
            for (Word w : podcast.getKeywords()) {
                Integer currentWeight = w.getWeight();
                if (weights.containsKey(w.getId())) {
                    currentWeight = currentWeight + weights.get(w.getId());
                }
                weights.put(w.getId(), currentWeight);
            }
        }
        return weights;
    }

    public Map<Integer, Integer> wordWeights(List<? extends Word> words){
        Map<Integer, Integer> weights = new HashMap<>();

            for (Word w : words){
                Integer currentWeight = w.getWeight();
                if (weights.containsKey(w.getId())){
                    currentWeight = currentWeight + weights.get(w.getId());
                }
                weights.put(w.getId(), currentWeight);
            }
        return weights;
    }


    // tf-idf = tf(term, doc) * idf(term)
    public Map<Integer, Double> calculateTfIdf(Map<Integer, Integer> weights, Map<Integer, Double> idfs){
        Map<Integer, Double> tfidfs = new HashMap<>();

        for (Integer id : weights.keySet()){
            Double idf = idfs.get(id);
            Double tf = weights.get(id)/(1.0*weights.size());

            Double tfidf = tf * idf;

            tfidfs.put(id, tfidf);
        }
        return tfidfs;
    }

    // Modified from:
    // http://stackoverflow.com/questions/520241/how-do-i-calculate-the-cosine-similarity-of-two-vectors
    public Double cosineSimilarity(Map<Integer, Double> a, Map<Integer, Double> b, List<? extends Word> allWords){
        Double product = 0.0;
        Double aNorm = 0.0;
        Double bNorm = 0.0;

        for (Word w : allWords){
            int id = w.getId();

            double aVal = 0.0;
            double bVal = 0.0;
            if (a.containsKey(id)){
                aVal = a.get(id);
            }
            if (b.containsKey(id)){
                bVal = b.get(id);
            }

            product += aVal * bVal;

            aNorm += Math.pow(aVal, 2);
            bNorm += Math.pow(bVal, 2);
        }

        return product/ (Math.sqrt(aNorm) * Math.sqrt(bNorm));
    }

    public static void main(String[] args) {

        // Sample ids for subscription
        String[] testIds = {"19919", "8184", "54", "960", "3185"};

        List<Integer> subscriptionIds = new ArrayList<>();
        for (String stringId : testIds){
            subscriptionIds.add(Integer.parseInt(stringId));
        }

        List<Podcast> subscriptions = new ArrayList<>();

        System.out.println("Loading subscriptions...");

        for (Integer id : subscriptionIds){
            subscriptions.add(DB.getInstance().getPodcast(id));
        }

        System.out.println("\nPODCAST SUBSCRIPTIONS:\n");

        for (Podcast podcast : subscriptions){
            System.out.println("#" +podcast.getId() +" -  "+podcast.getTitle());
            System.out.println("\t" + podcast.getDescription());
            System.out.println("\tkeywords: "+podcast.getKeywords());
            System.out.println("\tnamed entities: "+podcast.getNamedEntities());
            System.out.println("");
        }

        System.out.println("Fetching data...");

        System.out.println("Fetching podcasts");
        List<Podcast> allPodcasts = DB.getInstance().getPodcasts();

        System.out.println("Fetching keywords");
        List<Keyword> allKeywords = DB.getInstance().fetchKeywords();


        System.out.println(allPodcasts.size() +" podcasts");
        System.out.println(allKeywords.size() +" keywords");

        Recommender recommender = new Recommender();


        System.out.println("Calculating TF-IDF scores....");
        // IDF for all keywords and entities
        Map<Integer, Double> keywordIdfs = recommender.calculateIdf(allKeywords, allPodcasts.size());

        // TF (WEIGHTS) for subscription keywords and entities
        Map<Integer, Integer> subKeywordWeights = recommender.compositeKeywordWeights(subscriptions);

        // TFIDF for subscription keywords and entities
        Map<Integer, Double> subscriptionKeywordTfidfs = recommender.calculateTfIdf(subKeywordWeights, keywordIdfs);


        Map<Integer, Podcast> allPodcastsMap = new HashMap<>();
        for (Podcast p : allPodcasts){
            allPodcastsMap.put(p.getId(), p);
        }

        // NEIGHBOR BASED

        System.out.println("Fetching neighbors for each subscription.");
        PriorityQueue<RankedPodcast> ranked = new PriorityQueue<>();
        Map<Integer, Podcast> neighbors = new HashMap<>();
        for (Integer id : subscriptionIds){
            List<Integer> neighborIds = DB.getInstance().getNeighbors(id);
            System.out.println("neighbors: "+neighborIds.size());
            for (Integer neighborId : neighborIds){
                if (!subscriptionIds.contains(neighborId)) {
                    neighbors.put(neighborId, allPodcastsMap.get(neighborId));
                }
            }
        }

        System.out.println("\nRanking podcasts.");

        for (Integer id : neighbors.keySet()){
            Podcast neighbor = neighbors.get(id);

            Map<Integer, Integer> weights = recommender.wordWeights(neighbor.getKeywords());
            Map<Integer, Double> neighborTfidfs = recommender.calculateTfIdf(weights, keywordIdfs);

            Double rank = recommender.cosineSimilarity(subscriptionKeywordTfidfs, neighborTfidfs, allKeywords);
            ranked.add(new RankedPodcast(neighbor, rank));
        }

//        // NON_NEIGHBOR BASED

//        podcastQueue.addAll(allPodcasts);
//
//        for (int i = 0; i < MAX_THREAD_COUNT; i++){
//            new Thread(new Runnable() {
//                @Override
//                public void run() {
//                    while (!podcastQueue.isEmpty()) {
//                        Podcast p = podcastQueue.remove();
//                        if (podcastQueue.size() % 1000 == 0) {
//                            System.out.println(podcastQueue.size() + " podcasts left to rank");
//                        }
//                        Map<Integer, Integer> weights = recommender.wordWeights(p.getKeywords());
//                        Map<Integer, Double> tfidfs = recommender.calculateTfIdf(weights, keywordIdfs);
//
//                        Double rank = recommender.cosineSimilarity(subscriptionKeywordTfidfs, tfidfs, allKeywords);
//                        ranked.add(new RankedPodcast(p, rank));
//                    }
//
//                }
//            }).start();
//        }
//        while (ranked.size() != allPodcasts.size()) {
//            try {
//                Thread.sleep(100);
//            }
//            catch (Exception e) {
//                //
//            }
//        }

        System.out.println("\nRECOMMENDATIONS: ");
        int i = 0;
        while (i < 50) {
            RankedPodcast top = ranked.remove();
            if (!subscriptionIds.contains(top.getPodcast().getId())) {
                Podcast podcast = top.getPodcast();
                System.out.println("#" + podcast.getId() + " - " + podcast.getTitle() + " (rank = " + top.getRank() + "");
                System.out.println("\t" + podcast.getDescription());
                System.out.println("\tkeywords: " + podcast.getKeywords());
                System.out.println("\tnamed entities: " + podcast.getNamedEntities());
                System.out.println("");
                i++;
            }
        }
    }

}

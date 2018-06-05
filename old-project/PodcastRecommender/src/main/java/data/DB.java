package data;

import model.*;
import scraper.Feed;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Exchanger;
import java.util.function.Predicate;
import java.util.jar.Attributes;

/**
 * DB is a class to facilitate data persistence with a MySQL database as a singleton.
 * */
public class DB {

    private static Connection connection = null;
    private static DB instance = null;

    private static String connectionString = "jdbc:mysql://127.0.0.1:3307/PODCAST_RECOMMENDER";
    private static String user = "root";
    private static String password = "ly0qpZkP";

    private static Connection getConnection(){
        if (connection == null){
            try {
                connection = DriverManager.getConnection(connectionString, user, password);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return connection;
    }

    private DB(){
        connection = getConnection();
    }

    public static DB getInstance() {
        if (instance == null) {
            instance = new DB();
        }
        return instance;
    }

    /*************** CREATE/UPDATE SCHEMA ****************/

    private static void createTableFeed(Statement statement){
        try {
            statement.executeUpdate("CREATE TABLE FEED(id INTEGER PRIMARY KEY, url TEXT)");
        } catch (Exception e) {
            System.out.println(e.getLocalizedMessage());
            e.printStackTrace();

        }
    }

    private static void createTableItunesId(Statement statement){
        try {
            statement.executeUpdate("CREATE TABLE ITUNES_ID(" +
                    "itunes_id VARCHAR(20) PRIMARY KEY, url TEXT)");
        }
        catch (Exception e) {
            System.out.println(e.getClass() + ": " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void createTablePodcast(Statement statement){
        try {
            statement.executeUpdate("CREATE TABLE PODCAST(" +
                    "id INTEGER NOT NULL AUTO_INCREMENT, " +
                    "itunes_id TEXT, " +
                    "itunes_url TEXT, " +
                    "title TEXT, " +
                    "feed_url TEXT, " +
                    "category TEXT, " +
                    "description TEXT, " +
                    //"artwork_url TEXT" +
                    "PRIMARY KEY (id)" +
                    ")");
        }
        catch (Exception e){
            System.out.println("createTablePodcast: " +e.getClass() + ": " + e.getMessage());
            e.printStackTrace();

        }
    }

    private static void createTableNamedEntity(Statement statement){
        try {
            statement.executeUpdate("CREATE TABLE NAMEDENTITY (" +
                    "id INTEGER NOT NULL AUTO_INCREMENT, " +
                    "name VARCHAR(255) UNIQUE," +
                    "PRIMARY KEY(id))" );
        }
        catch (Exception e){
            System.out.println("createTableNamedEntity"+e.getClass() + ": " + e.getMessage());
            e.printStackTrace();

        }
    }

    private static void createTableKeyword(Statement statement){
        try {
            statement.executeUpdate("CREATE TABLE KEYWORD (" +
                    "id INTEGER NOT NULL AUTO_INCREMENT, " +
                    "word VARCHAR(255) UNIQUE," +
                    "category TEXT," +
                    "PRIMARY KEY(id))" );
        }
        catch (Exception e){
            System.out.println("createTableKeyword"+e.getClass() + ": " + e.getMessage());
            e.printStackTrace();

        }
    }

    private static void createTableNamedEntityPodcast(Statement statement){
        try {
            statement.executeUpdate("CREATE TABLE NAMEDENTITY_PODCAST (" +
                    "entity_id INTEGER, " +
                    "podcast_id INTEGER, " +
                    "weight INTEGER, " +
                    "PRIMARY KEY (entity_id, podcast_id)," +
                    "FOREIGN KEY(podcast_id) REFERENCES PODCAST(id), " +
                    "FOREIGN KEY(entity_id) REFERENCES NAMEDENTITY(id)" +
                    ")" );
        }
        catch (Exception e){
            System.out.println("createTableNamedEntityPodcast"+e.getClass() + ": " + e.getMessage());
            e.printStackTrace();

        }
    }

    private static void createTableKeywordPodcast(Statement statement){
        try {
            statement.executeUpdate("CREATE TABLE KEYWORD_PODCAST (" +
                    "word_id INTEGER, " +
                    "podcast_id INTEGER, " +
                    "weight INTEGER," +
                    "PRIMARY KEY (word_id, podcast_id)," +
                    "FOREIGN KEY (word_id) REFERENCES KEYWORD(id)," +
                    "FOREIGN KEY (podcast_id) REFERENCES PODCAST(id)" +
                    ")" );
        }
        catch (Exception e){
            System.out.println("createTableKeywordPodcast"+e.getClass() + ": " + e.getMessage());
            e.printStackTrace();

        }
    }

    /******************** PUBLIC ****************************/

    public List<String> getItunesIds() {
        List<String> ids = new ArrayList<>();

        Statement statement = null;
        try {
            statement = connection.createStatement();
            ResultSet results = statement.executeQuery("SELECT itunes_id FROM ITUNES_ID");

            while (results.next()){
                String id = results.getString("itunes_id");
                ids.add(id);
            }
        }
        catch (Exception e){
            System.out.println("getPodcast: " + e.getClass() + ": " + e.getMessage());
            e.printStackTrace();

        }

        return ids;
    }

    public List<String> getNewItunesIds() {
        List<String> ids = new ArrayList<>();

        Statement statement = null;
        try {
            statement = connection.createStatement();
            ResultSet results = statement.executeQuery("SELECT itunes_id FROM ITUNES_ID WHERE processed = false");

            while (results.next()){
                String id = results.getString("itunes_id");
                ids.add(id);
            }
        }
        catch (Exception e){
            System.out.println("getNewItunesId: " + e.getClass() + ": " + e.getMessage());
            e.printStackTrace();

        }

        return ids;
    }

    public List<Feed> getFeeds(int limit) {
        List<Feed> feeds = new ArrayList<>();

        Statement statement = null;
        try {
            statement = connection.createStatement();
            ResultSet results = null;
            if (limit < 0){
                results = statement.executeQuery("SELECT itunes_id, url FROM FEED_URL where url is not null");
            }
            else {
                results = statement.executeQuery("SELECT itunes_id, url FROM FEED_URL where url is not null LIMIT " + limit);
            }

            if (results != null) {
                while (results.next()) {
                    Feed feed = new Feed();
                    feed.itunesId = results.getString("itunes_id");
                    feed.url = results.getString("url");
                    feeds.add(feed);
                }
            }
        }
        catch (Exception e){
            System.out.println("getFeedUrls: " + e.getClass() + ": " + e.getMessage());
            e.printStackTrace();
        }

        return feeds;
    }

    public List<String> getPodcastItunesIds() {
        List<String> ids = new ArrayList<>();

        Statement statement = null;
        try {
            statement = connection.createStatement();
            ResultSet results = statement.executeQuery("SELECT itunes_id FROM PODCAST");

            while (results.next()){
                String id = results.getString("itunes_id");
                ids.add(id);
            }
        }
        catch (Exception e){
            System.out.println("getPodcastItunesIds: " + e.getClass() + ": " + e.getMessage());
            e.printStackTrace();

        }

        return ids;
    }

    public List<String> getFeedUrlItunesIds() {
        List<String> ids = new ArrayList<>();

        Statement statement = null;
        try {
            statement = connection.createStatement();
            ResultSet results = statement.executeQuery("SELECT itunes_id FROM FEED_URL");

            while (results.next()){
                String id = results.getString("itunes_id");
                ids.add(id);
            }
        }
        catch (Exception e){
            System.out.println("getPodcastItunesIds: " + e.getClass() + ": " + e.getMessage());
            e.printStackTrace();

        }

        return ids;
    }

    public List<String> getUnprocessedItunesIds(){
        List<String> ids = new ArrayList<>();

        Statement statement = null;
        try {
            System.out.println("querying for unprocessed ids...");

            statement = connection.createStatement();
            ResultSet results = statement.executeQuery("" +
                    "SELECT itunes_id FROM Itunes_id " +
                    "WHERE itunes_id NOT IN (SELECT itunes_id FROM Podcast)" +
                    "ORDER BY itunes_id");

            System.out.println("query finished....");

            while (results.next()){
                String id = results.getString("itunes_id");
                ids.add(id);
            }
        }
        catch (Exception e){
            System.out.println("getUnprocessedItunesIds: " + e.getClass() + ": " + e.getMessage());
            e.printStackTrace();
        }

        return ids;

    }

    public Podcast getPodcast(Integer id){
        Statement statement = null;
        Podcast podcast = null;
        try {
            statement = connection.createStatement();
            ResultSet results = statement.executeQuery("SELECT * FROM PODCAST WHERE id = " + id);

            while (results.next()){
                podcast = new Podcast();
                populatePodcastWithResultSet(podcast, results);
                break; // ensure that only the first is taken -- though there should only be one
            }
        }
        catch (Exception e){
            System.out.println("getPodcast: " + e.getClass() + ": " + e.getMessage());
            e.printStackTrace();

        }
        return podcast;
    }

    public List<Podcast> getPodcasts(){
        Statement statement = null;
        List<Podcast> podcasts = new ArrayList<>();
        try {
            statement = connection.createStatement();
            ResultSet results = statement.executeQuery("SELECT * FROM PODCAST");

            while (results.next()){
                Podcast podcast = new Podcast();
                populatePodcastWithResultSet(podcast, results);
                podcasts.add(podcast);
            }
        }
        catch (Exception e){
            System.out.println("getPodcast: " + e.getClass() + ": " + e.getMessage());
            e.printStackTrace();

        }
        return podcasts;
    }


    public Podcast save(Podcast podcast){
        Statement statement = null;
        try {
            statement = connection.createStatement();

            // SAVE PODCAST
            if (podcast.getId() == null) {
                //System.out.println("podcast title: " + podcast.getTitle() + ", description: "+ podcast.getDescription());
                statement.executeUpdate("" +
                        "INSERT INTO PODCAST(title, itunes_id, itunes_url, feed_url, category, description) " +
                        "VALUES('"+podcast.getTitle().replace("'", "''")+"','"+podcast.getItunesId()+"', '"+podcast.getItunes_url()+"', '"
                        +podcast.getFeedUrl()+"', '"+podcast.getCategory()+"', '"+podcast.getDescription().replace("'", "''")+"')"
                );

                ResultSet keys = statement.getGeneratedKeys();
                keys.next();
                podcast.setId(keys.getInt(1));
            } else {
                statement.executeUpdate(""+
                        "UPDATE PODCAST" +
                        "SET itunes_id = '"+podcast.getItunesId()+"', itunes_url = '"+podcast.getItunes_url()+"', " +
                        "feed_url = '"+podcast.getFeedUrl()+"', category = '"+podcast.getCategory()+"', " +
                        "description = '"+podcast.getDescription()+"'" +
                        "WHERE id = " + podcast.getId()
                );
            }

            // SAVE KEYWORDS
            saveKeywords(podcast.getKeywords(), podcast.getId());

            // SAVE NAMED ENTITIES
            saveEntities(podcast.getNamedEntities(), podcast.getId());

            System.out.println("Saved Podcast: " + podcast.getId());
        }
        catch (Exception e){
            System.out.println("save(Podcast): " + e.getClass() + ": " + e.getMessage());
            e.printStackTrace();

        }
        return podcast;
    }

    public void save(List<Podcast> podcasts){
        for (Podcast p : podcasts){
            save(p);
        }
    }

    public void savePodcastToPodcast2(Podcast podcast){
        Statement statement = null;
        try {
            PreparedStatement preparedKeywordStatement = connection.prepareStatement("" +
                    "INSERT INTO PODCAST2 (itunes_id, feed_url, title, category, description, author, raw_text) " +
                    "VALUES(?, ?, ?, ?, ?, ?, ?)");

            preparedKeywordStatement.setString(1, "" + podcast.getItunesId());
            preparedKeywordStatement.setString(2, podcast.getFeedUrl());
            preparedKeywordStatement.setString(3, podcast.getTitle());
            preparedKeywordStatement.setString(4, podcast.getCategory());
            preparedKeywordStatement.setString(5, podcast.getDescription());
            preparedKeywordStatement.setString(6, podcast.getAuthors());
            preparedKeywordStatement.setString(7, podcast.getRawText());

            preparedKeywordStatement.execute();
        }
        catch (Exception e){
            System.out.println("savePodcastToPodcast2(Podcast): " + e.getClass() + ": " + e.getMessage());
            e.printStackTrace();
        }
    }

    public void saveEntities(List<NamedEntity> entities, Integer podcastId){

        for (NamedEntity e : entities){
            save(e, podcastId);
        }
    }

    public void saveKeywords(List<Keyword> keywords, Integer podcastId){
        for (Keyword k : keywords){
            save(k, podcastId);
        }
    }

    public void save(Keyword keyword, Integer podcastId){
        Statement statement = null;
        try {
            statement = connection.createStatement();
            // SAVE KEYWORD
            if (keyword.getId() == null) {

                ResultSet existentKey = statement.executeQuery("SELECT * FROM KEYWORD WHERE LOWER(word) like '"+keyword.getWord().toLowerCase().replace("'", "''")+"'");
                if (existentKey.next()){
                    keyword.setId(existentKey.getInt("id"));
                }
                else {

                    statement.executeUpdate("" +
                            "INSERT INTO KEYWORD(word, category) " +
                            "VALUES('" + keyword.getWord().replace("'", "''") + "', '" + keyword.getCategory() + "')");

                    ResultSet keys = statement.getGeneratedKeys();
                    keys.next();
                    keyword.setId(keys.getInt(1));
                }
                try {
                    statement.executeUpdate("INSERT INTO KEYWORD_PODCAST(podcast_id, word_id, weight) " +
                            "VALUES(" + podcastId + ", " + keyword.getId() + ", " + keyword.getWeight() + ")");
                }
                catch (Exception e){
                    // do nothing - suppress stack trace on duplicate
                }
            } else {

            }
            //System.out.println("\tSaved keyword: " + keyword.getWord() + " ("+keyword.getId()+")");
        }
        catch (Exception e){
            System.out.println("save(Keyword): " + e.getClass() + ": " + e.getMessage());
            e.printStackTrace();

        }
    }

    public void save(NamedEntity entity, Integer podcastId){
        Statement statement = null;
        try {
            statement = connection.createStatement();
            // SAVE ENTITY
            if (entity.getId() == null) {

                ResultSet existentKey = statement.executeQuery("SELECT * FROM NAMEDENTITY WHERE LOWER(name) like '"+entity.getName().toLowerCase().replace("'", "''")+"'");
                if (existentKey.next()){
                    entity.setId(existentKey.getInt("id"));
                }
                else {

                    statement.executeUpdate("" +
                            "INSERT INTO NAMEDENTITY(name) " +
                            "VALUES('" + entity.getName().replace("'", "''") + "')");

                    ResultSet keys = statement.getGeneratedKeys();
                    keys.next();
                    entity.setId(keys.getInt(1));
                }

                statement.executeUpdate("INSERT INTO NAMEDENTITY_PODCAST(podcast_id, entity_id, weight) " +
                        "VALUES(" + podcastId + ", " + entity.getId() + ", "+ entity.getWeight() + ")");
            } else {

            }
        }
        catch (Exception e){
            System.out.println("save(NamedEntity): " + e.getClass() + ": " + e.getMessage());
            e.printStackTrace();
        }
    }

    /*********************** BATCH SAVE OPERATIONS *******************************/

    public void batchSavePodcasts(Queue<Podcast> podcasts){
        try {
            PreparedStatement preparedKeywordStatement = connection.prepareStatement("" +
                    "INSERT INTO PODCAST(title, itunes_id, itunes_url, feed_url, category, description) " +
                    "VALUES(?, ?, ?, ?, ?, ?)");
            while (!podcasts.isEmpty()){
                Podcast podcast = podcasts.remove();

                if (podcast.getId() == null) {
                    preparedKeywordStatement.setString(1, podcast.getTitle());
                    preparedKeywordStatement.setString(2, "" + podcast.getItunesId());
                    preparedKeywordStatement.setString(3, podcast.getItunes_url());
                    preparedKeywordStatement.setString(4, podcast.getFeedUrl());
                    preparedKeywordStatement.setString(5, podcast.getCategory());
                    preparedKeywordStatement.setString(6, podcast.getDescription());

                    preparedKeywordStatement.execute();

                    ResultSet generatedKeys = preparedKeywordStatement.getGeneratedKeys();

                    if (generatedKeys.next()) {
                        podcast.setId(generatedKeys.getInt(1));
                    }
                }
            }
        }
        catch(Exception e){
            // e.printStackTrace();
        }
    }

    public void batchSavePodcastsToFeedUrl(Queue<Podcast> podcasts){
        try {
            PreparedStatement preparedKeywordStatement = connection.prepareStatement("" +
                    "INSERT INTO FEED_URL(itunes_id, url) " +
                    "VALUES(?, ?)");
            while (!podcasts.isEmpty()){
                Podcast podcast = podcasts.remove();

//                if (podcast.getId() == null) {
                preparedKeywordStatement.setString(1, "" + podcast.getItunesId());
                preparedKeywordStatement.setString(2, podcast.getFeedUrl());

                preparedKeywordStatement.execute();

//                    ResultSet generatedKeys = preparedKeywordStatement.getGeneratedKeys();

//                    if (generatedKeys.next()) {
//                        podcast.setId(generatedKeys.getInt(1));
//                    }
//                }
            }
        }
        catch(Exception e){
            e.printStackTrace();
        }
    }

    public void savePodcastToFeedUrl(Podcast podcast){
        try {
            PreparedStatement preparedKeywordStatement = connection.prepareStatement("" +
                    "INSERT INTO FEED_URL(itunes_id, url) " +
                    "VALUES(?, ?)");

                preparedKeywordStatement.setString(1, "" + podcast.getItunesId());
                preparedKeywordStatement.setString(2, podcast.getFeedUrl());

                preparedKeywordStatement.execute();
        }
        catch(Exception e){
            e.printStackTrace();
        }
    }

    public void batchSaveKeywords(Map<String, Keyword> keywords){
        try {
            PreparedStatement preparedKeywordStatement = connection.prepareStatement("INSERT INTO KEYWORD(word, category) VALUES (?, ?)");

            for (Keyword keyword : keywords.values()) {
                if (keyword.getId() == null) {
                    try {
                        preparedKeywordStatement.setString(1, keyword.getWord());
                        preparedKeywordStatement.setString(2, keyword.getCategory());
                        preparedKeywordStatement.execute();
                        ResultSet generatedKeys = preparedKeywordStatement.getGeneratedKeys();

                        if (generatedKeys.next()) {
                            keyword.setId(generatedKeys.getInt(1));
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        catch(Exception e){
            // e.printStackTrace();
        }
    }

    public void batchSaveEntities(Map<String, NamedEntity> entities){
        try {
            PreparedStatement preparedKeywordStatement = connection.prepareStatement("INSERT INTO NAMEDENTITY(name) VALUES (?)");


            for (NamedEntity entity : entities.values()) {
                if (entity.getId() == null) {
                    try {
                        preparedKeywordStatement.setString(1, entity.getName());
                        preparedKeywordStatement.execute();
                        ResultSet generatedKeys = preparedKeywordStatement.getGeneratedKeys();

                        if (generatedKeys.next()) {
                            entity.setId(generatedKeys.getInt(1));
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }

        }
        catch(Exception e){
            // e.printStackTrace();
        }
    }

    public void batchSaveEntityPodcasts(Queue<EntityPodcast> entityPodcasts){
        try {
            PreparedStatement preparedKeywordStatement = connection.prepareStatement("" +
                    "INSERT INTO NAMEDENTITY_PODCAST(podcast_id, entity_id, weight) " +
                    "VALUES(?, ?, ?)");

            while (!entityPodcasts.isEmpty()){
                try {
                    EntityPodcast ep = entityPodcasts.remove();

                    preparedKeywordStatement.setInt(1, ep.getPodcast().getId());
                    preparedKeywordStatement.setInt(2, ep.getEntity().getId());
                    preparedKeywordStatement.setInt(3, ep.getWeight());

                    preparedKeywordStatement.execute();
                }
                catch (Exception e) {
                   //  e.printStackTrace();
                }
            }
        }
        catch(Exception e){
            // e.printStackTrace();
        }
    }

    public void batchSaveKeywordPodcasts(Queue<KeywordPodcast> keywordPodcasts){
        try {
            PreparedStatement preparedKeywordStatement = connection.prepareStatement("" +
                    "INSERT INTO KEYWORD_PODCAST(podcast_id, word_id, weight) " +
                    "VALUES(?, ?, ?)");

            while (!keywordPodcasts.isEmpty()){

                try {
                    KeywordPodcast kp = keywordPodcasts.remove();

                    preparedKeywordStatement.setInt(1, kp.getPodcast().getId());
                    preparedKeywordStatement.setInt(2, kp.getKeyword().getId());
                    preparedKeywordStatement.setInt(3, kp.getWeight());

                    preparedKeywordStatement.execute();
                }
                catch (Exception e){
                    // e.printStackTrace();
                }
            }
        }
        catch(Exception e){
            // e.printStackTrace();
        }
    }


    /******************** HELPERS ***************************/


    private void populatePodcastWithResultSet(Podcast podcast, ResultSet results) throws java.sql.SQLException {
        podcast.setId(results.getInt("id"));
        podcast.setItunes_url(results.getString("itunes_url"));
        podcast.setTitle(results.getString("title"));
        podcast.setFeedUrl(results.getString("feed_url"));
        podcast.setCategory(results.getString("category"));
        podcast.setDescription(results.getString("description"));

        podcast.setKeywords(fetchKeywordsForPodcast(podcast));
        podcast.setNamedEntities(fetchNamedEntitiesForPodcast(podcast));
    }


    private List<Keyword> fetchKeywordsForPodcast(Podcast podcast) throws java.sql.SQLException {
        if (podcast.getId() == null){
            return null;
        }

        List<Keyword> keywords = new ArrayList<>();
        Statement statement = connection.createStatement();
        ResultSet results = statement.executeQuery(
                "SELECT K.id as id, K.word as word, K.category as category, KP.weight as weight " +
                        "FROM KEYWORD AS K, KEYWORD_PODCAST AS KP, PODCAST AS P " +
                        "WHERE P.id = KP.podcast_id " +
                        "AND K.id = KP.word_id " +
                        "AND P.id = " + podcast.getId() +
                        " ORDER BY K.id");

        while (results.next()){
            Keyword k = new Keyword();

            k.setId(results.getInt("id"));
            k.setCategory(results.getString("category"));
            k.setWeight(results.getInt("weight"));
            k.setWord(results.getString("word"));

            keywords.add(k);

        }

        return keywords;
    }

    private List<NamedEntity> fetchNamedEntitiesForPodcast(Podcast podcast)  throws java.sql.SQLException {

        if (podcast.getId() == null){
            return null;
        }

        List<NamedEntity> entities = new ArrayList<>();
        Statement statement = connection.createStatement();
        ResultSet results = statement.executeQuery(
                "SELECT E.id as id, E.name, EP.weight as weight " +
                        "FROM NAMEDENTITY AS E, PODCAST AS P, NAMEDENTITY_PODCAST AS EP " +
                        "WHERE P.id = EP.podcast_id " +
                        "AND E.id = EP.entity_id " +
                        "AND P.id = " + podcast.getId() +
                        " ORDER BY E.id"
        );

        while (results.next()){
            NamedEntity e = new NamedEntity();

            e.setId(results.getInt("id"));
            e.setName(results.getString("name"));
            e.setWeight(results.getInt("weight"));

            entities.add(e);
        }

        return entities;
    }

    public List<Keyword> fetchKeywords() {

        try {
            List<Keyword> keywords = new ArrayList<>();
            Statement statement = connection.createStatement();
            ResultSet results = statement.executeQuery(
                    "SELECT K.id as id, K.word as word, K.category as category, SUM(KP.weight) as weight, COUNT(*) as df " +
                            "FROM KEYWORD AS K, KEYWORD_PODCAST AS KP " +
                            "WHERE K.id = KP.word_id " +
                            " GROUP BY K.id" +
                            " ORDER BY K.id");

            while (results.next()) {
                Keyword k = new Keyword();

                k.setId(results.getInt("id"));
                k.setCategory(results.getString("category"));
                k.setWeight(results.getInt("weight"));
                k.setWord(results.getString("word"));
                k.setDf(results.getInt("df"));

                keywords.add(k);

            }

            return keywords;
        }
        catch (SQLException e){
            e.printStackTrace();
            return null;
        }
    }

    public List<NamedEntity> fetchNamedEntities() {

        try {
            List<NamedEntity> entities = new ArrayList<>();
            Statement statement = connection.createStatement();
            ResultSet results = statement.executeQuery(
                    "SELECT E.id as id, E.name as name, SUM(EP.weight) as weight, COUNT(*) as df " +
                            "FROM NAMEDENTITY AS E, NAMEDENTITY_PODCAST AS EP " +
                            "WHERE E.id = EP.entity_id " +
                            " GROUP BY E.id" +
                            " ORDER BY E.id");

            while (results.next()) {
                NamedEntity e = new NamedEntity();

                e.setId(results.getInt("id"));
                e.setWeight(results.getInt("weight"));
                e.setName(results.getString("name"));
                e.setDf(results.getInt("df"));

                entities.add(e);

            }

            return entities;
        }
        catch (SQLException e){
            e.printStackTrace();
            return null;
        }
    }

    public List<Integer> getNeighbors(Integer podcastId) {
        try {
            List<Integer> ids = new ArrayList<>();
            Statement statement = connection.createStatement();
            ResultSet results = statement.executeQuery("" +
                    "SELECT DISTINCT EP.podcast_id as id " +
                    "FROM NAMEDENTITY_PODCAST AS EP, NAMEDENTITY AS E " +
                    "WHERE EP.entity_id IN (SELECT entity_id FROM NAMEDENTITY_PODCAST WHERE podcast_id = " + podcastId + ") " +
                    "AND EP.podcast_id != " + podcastId +" "+
                    "AND LENGTH(E.name) > 5 " +
                    "LIMIT 1000");

            while (results.next()) {
               ids.add(results.getInt("id"));
            }

            return ids;
        }
        catch (SQLException e){
            e.printStackTrace();
            return null;
        }
    }


    /********************* QUERIES & OPERATIONS **********************/

    public static boolean insertFeedUrl(String url){
        Statement statement = null;
        try {
            statement = getConnection().createStatement();
        }
        catch (Exception e){
            System.out.println(e.getLocalizedMessage());
            e.printStackTrace();

            return false;
        }
        if (statement != null) {
            try {
                statement.executeUpdate("INSERT INTO FEED (url) VALUES ('" + url + "')");
            } catch (Exception e) {
                System.out.println(e.getLocalizedMessage());
                e.printStackTrace();

                return false;
            }
        }
        return true;
    }

    public static boolean insertItunesId(String id, String url){
        Statement statement = null;
        try {
            statement = getConnection().createStatement();
        }
        catch (Exception e){
            System.out.println(e.getLocalizedMessage());
            e.printStackTrace();

            return false;
        }
        if (statement != null) {
            try {
                statement.executeUpdate("INSERT INTO ITUNES_ID (itunes_id, url) VALUES ('" + id + "', '"+url+"')");
            } catch (Exception e) {
                //System.out.println(e.getLocalizedMessage());
                e.printStackTrace();

                return false;
            }
        }
        return true;
    }


    /********************** DRIVER *************************/

    public static void main(String[] args) {
        Statement statement = null;
        try {
            statement = getConnection().createStatement();

            if (statement != null) {
                //DROP TABLES
                statement.executeUpdate("DROP TABLE IF EXISTS NAMEDENTITY_PODCAST");
                statement.executeUpdate("DROP TABLE IF EXISTS KEYWORD_PODCAST");
                statement.executeUpdate("DROP TABLE IF EXISTS NAMEDENTITY");
                statement.executeUpdate("DROP TABLE IF EXISTS KEYWORD");
                statement.executeUpdate("DROP TABLE IF EXISTS PODCAST");
//                statement.executeUpdate("DROP TABLE IF EXISTS ITUNES_ID");
//                statement.executeUpdate("DROP TABLE IF EXISTS FEED");

                // CREATE TABLES
//                createTableFeed(statement);
//                createTableItunesId(statement);
                createTablePodcast(statement);
                createTableKeyword(statement);
                createTableNamedEntity(statement);

                createTableKeywordPodcast(statement);
                createTableNamedEntityPodcast(statement);
            }
        }
        catch (SQLException e){
            System.out.println(e.getLocalizedMessage());
            e.printStackTrace();
        }
    }
}

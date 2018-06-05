package edu.utdallas.spark

import com.mongodb.spark._
import com.mongodb.spark.config.{ReadConfig, WriteConfig}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.bson
import org.bson.{BsonDocument, Document}
import org.mongodb.scala.{Subscription, MongoClient, Observer}
import org.mongodb.scala.bson._


import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.MutableList


/**
  * access mongo shell on google ssh: $ mongo admin --username root -p
  * Admin password (Temporary): cjfrDr3h
  *
  * podcastdb
  *   user: "client"
  *   pwd: 19dbw964s-27snw224081
  *
  *   gcloud passphrase for ssh keygen: bigdata
  *
  *
  * backup command to folder /dump:
  * $ mongodump -h 127.0.0.1:27017 --authenticationDatabase admin --username root --password cjfrDr3h -d podcastdb
  *
*mongodump -h 127.0.0.1:27017 --authenticationDatabase admin --username root --password cjfrDr3h -d podcastdb
  *
  *
  * SSH tunnel to view sparkUI
  * $ gcloud compute ssh --zone=us-central1-a --ssh-flag="-D 1080" --ssh-flag="N" --ssh-flag="n" podcast-recommender-cluster-m
  * $ /Applications/Google\ Chrome.app/Contents/MacOS/Google\ Chrome --proxy-server="socks5://localhost:1080" --host-resolver-rules="MAP * 0.0.0.0 , EXCLUDE localhost" --user-data-dir=/tmp/
  *
  *
  *
  */

case class FeedUrl(_id: String, itunesId: String, feedUrl: String)

object JobRunner {

  /**
    * Job: TEST
    *
    */

  def Test(): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("IQ Simple Spark App").setMaster("local[*]"))
    val rdd = sc.parallelize(Seq(1, 2, 3, 4, 5))
    println(rdd.count())
    sc.stop()
  }

    /**
      * Job: PARSE FEEDS FROM FILE
      *
      * @param local whether the jobs should run locally, accessing the local mongodb; if not, it should connect to remote
      *              db and assume it is running on google cluster.
      */

    def ParseFeedsFromFile(local:Boolean, filePath:String): Unit ={
      var dbAddress = "mongodb://127.0.0.1/podcastdb.podcast" // local
      var dbConnectionString =  "mongodb://127.0.0.1/"
      var fileUri = filePath

      if (!local) {
        dbAddress = "mongodb://client:19dbw964s-27snw224081@104.154.182.252/podcastdb.podcast" // remote
        //dbConnectionString = "mongodb://client:19dbw964s-27snw224081@104.154.182.252/"
      }

      val spark = SparkSession.builder()
        .master("local")
        .appName("FeedParser")
        .config("spark.mongodb.input.uri", dbAddress)
        .config("spark.mongodb.output.uri",  dbAddress)
        .getOrCreate()

      val sc = spark.sparkContext

      val readConfig = ReadConfig(Map("uri" -> dbAddress))
      val writeConfig = WriteConfig(Map("uri" -> dbAddress))

      val podcasts = sc.textFile(fileUri)
        .map(line => line.split("::"))
        .map(col => (col(0), col(1))) // (itunesId, url)
        .map(row => {

        val podcast: Podcast = RSSParser.processFeed(row._2)
        val podcastDoc = new Document

        if (podcast != null){
          podcastDoc.put("fetchSuccess", true)

          var episodes: MutableList[Document] = MutableList()

          for (episode: Episode <- podcast.getEpisodes){
            val epDoc = new Document

            epDoc.append("title", episode.getTitle)
            epDoc.append("description", episode.getDescription)

            episodes += epDoc
          }

          podcastDoc.put("title", podcast.getTitle)
          podcastDoc.put("authors", podcast.getAuthors)
          podcastDoc.put("itunesId", row._1)
          podcastDoc.put("_id", row._1)
          podcastDoc.put("feedUrl", podcast.getFeedUrl)
          podcastDoc.put("description", podcast.getDescription)
          podcastDoc.put("category", podcast.getCategory)
          podcastDoc.put("episodes", episodes.asJava)

        }
        else {
          podcastDoc.put("fetchSuccess", false)
        }
        podcastDoc
      })

      MongoSpark.save(podcasts, writeConfig);

      sc.stop()
    }


  def PopulateDBWithFeedUrls(local: Boolean): Unit = {
    var dbAddress = "mongodb://127.0.0.1/podcastdb.feedUrl"

    if (!local) {
      dbAddress = "mongodb://client:19dbw964s-27snw224081@104.154.182.252/podcastdb.feedUrl"
    }

    var fileUri = "/usr/local/spark-data/feed_url.csv" // local

    val spark = SparkSession.builder()
      .master("local")
      .appName("FeedParser")
      .config("spark.mongodb.input.uri", dbAddress)
      .config("spark.mongodb.output.uri",  dbAddress)
      .getOrCreate()

    val sc = spark.sparkContext

    val writeConfig = WriteConfig(Map("uri" -> dbAddress))

    val feedUrls = sc.textFile(fileUri)
      .map(line => line.split("::"))
      .map(col => {

        val doc = new Document

        doc.put("itunesId", col(0))
        doc.put("feedUrl", col(1))

        (col(0), doc)
    })
      .reduceByKey((x, y) => {
        println(x)
        x
      })


    MongoSpark.save(feedUrls, writeConfig);
    sc.stop()
  }

  def ProcessKeywords(local:Boolean): Unit ={

    // DB Connection Setup

    println("Configure Spark Context and DB.")

    var dbAddress = "mongodb://127.0.0.1/podcastdb" // local
//    var master = "local"

    if (!local) {
      dbAddress = "mongodb://client:19dbw964s-27snw224081@104.154.182.252/podcastdb" // remote
//      master =  "spark://10.128.0.5:7077"
    }

    val readAddress = dbAddress + ".podcast"
    val writePodcastKeywordAddress = dbAddress + ".keyword_podcast_temp"

    val spark = new SparkConf().setAppName("Process Keywords")
    val sc = new SparkContext(spark)

    val readConfig = ReadConfig(Map("uri" -> readAddress))
    val writePodcastKeywordsConfig = WriteConfig(Map("uri" -> writePodcastKeywordAddress))


    println("Read podcasts from DB")

    // Get all podcast documents

    val podcastDocs = MongoSpark.load(sc, readConfig)
      .filter(doc => doc.get("fetchSuccess").asInstanceOf[Boolean])


    // Create texts from documents

    val podcastKeywords = podcastDocs
      .map(podcastDoc => {

        val itunesId = podcastDoc.getString("itunesId")

        println("\tProcess podcast " + itunesId)

        var text = podcastDoc.getString("title") + ". " + podcastDoc.getString("description") + ". "
        val episodes = podcastDoc.get("episodes").asInstanceOf[java.util.ArrayList[Document]]
        episodes.foreach(episodeDoc => {
          text = text + episodeDoc.getString("title") + ". "
          val episodeDescription = episodeDoc.getString("description")
          if (episodeDescription != null && episodeDescription.trim.length > 0){
            text = text + episodeDescription + ". "
          }
        })

        val words = TextAnalyzer.extractKeywords(text)

        println("\t\t Extracted " + words.size + " keywords from podcast " + itunesId)

        val keywords = words.map(keyValPair => {
          val word = keyValPair._1
          val weight = keyValPair._2

          val wordDoc = new Document
          wordDoc.put("word", word)
          wordDoc.put("weight", weight)
          wordDoc.put("tf", weight.toDouble / words.size.toDouble) // TF score = (# of occurences of t)/(# of terms in d)

          wordDoc
        }).asJava

        val podcastKeywordsDoc = new Document
        podcastKeywordsDoc.put("_id", itunesId)
        podcastKeywordsDoc.put("itunesId", itunesId)
        podcastKeywordsDoc.put("keywords", keywords)

        podcastKeywordsDoc
      })

    podcastKeywords.cache()

    println("Saving " + podcastKeywords.count() + " podcasts to keyword_podcast_temp")

    MongoSpark.save(podcastKeywords, writePodcastKeywordsConfig)

    sc.stop()
  }

  // This version hopefully will perform better on the cluster because it ensures that the CoreNLP pipeline is
  // initialized only once per pertition.
  def ProcessKeywords2(local:Boolean): Unit ={

    // DB Connection Setup

    var dbAddress = "mongodb://127.0.0.1/podcastdb" // local

    if (!local) {
      dbAddress = "mongodb://client:19dbw964s-27snw224081@104.154.182.252/podcastdb" // remote
    }

    val readAddress = dbAddress + ".podcast"
    val writePodcastKeywordAddress = dbAddress + ".keyword_podcast_temp"

    // Spark Config

    val spark = new SparkConf().setAppName("Process Keywords")
    if (local){ spark.setMaster("local") }
    val sc = new SparkContext(spark)

    val readConfig = ReadConfig(Map("uri" -> readAddress))
    val writePodcastKeywordsConfig = WriteConfig(Map("uri" -> writePodcastKeywordAddress))


    // Get all podcast documents

    val podcastDocs = MongoSpark.load(sc, readConfig)
      .filter(doc => doc.get("fetchSuccess").asInstanceOf[Boolean])


    // Create texts from documents

    val podcastKeywords = podcastDocs
      .mapPartitions(docs => {

        val analyzer = new TextAnalyzerPipeline()
        val processorPipeline = analyzer.createPipeline()

        val docsToInsert = docs.map(podcastDoc => {
          val itunesId = podcastDoc.getString("itunesId")

          var text = podcastDoc.getString("title") + ". " + podcastDoc.getString("description") + ". "
          val episodes = podcastDoc.get("episodes").asInstanceOf[java.util.ArrayList[Document]]

          episodes.foreach(episodeDoc => {
            text = text + episodeDoc.getString("title") + ". "
            val episodeDescription = episodeDoc.getString("description")
            if (episodeDescription != null && episodeDescription.trim.length > 0) {
              text = text + episodeDescription + ". "
            }
          })

          val words = analyzer.extractKeywords(text, processorPipeline)

          val keywords = words.map(keyValPair => {
            val word = keyValPair._1
            val weight = keyValPair._2

            var wordDoc = new Document
            wordDoc.put("word", word)
            wordDoc.put("weight", weight)
            wordDoc.put("tf", weight.toDouble / words.size.toDouble) // TF score = (# of occurences of t)/(# of terms in d)

            wordDoc
          }).asJava

          var podcastKeywordsDoc = new Document
          podcastKeywordsDoc.put("_id", itunesId)
          podcastKeywordsDoc.put("itunesId", itunesId)
          podcastKeywordsDoc.put("keywords", keywords)

          podcastKeywordsDoc
        })

        docsToInsert
      })

    //podcastKeywords.foreach(doc => println( "\t" + doc.getString("itunesId")))

    MongoSpark.save(podcastKeywords, writePodcastKeywordsConfig)

    sc.stop()
  }

    def CalculateIdfs(local:Boolean): Unit ={

      // DB Connection Setup

      var dbAddress = "mongodb://127.0.0.1/podcastdb" // local

      if (!local) {
        dbAddress = "mongodb://client:19dbw964s-27snw224081@104.154.182.252/podcastdb" // remote
      }

      val readAddress = dbAddress + ".keyword_podcast_temp"
      val writePodcastKeywordAddress = dbAddress + ".keyword_podcast"

      // Spark Config

      val ssBuilder = SparkSession.builder()
        .appName("FeedParser")
      if(local){ssBuilder.master("local")}
      val spark = ssBuilder.getOrCreate()

      val sc = spark.sparkContext

      val readConfig = ReadConfig(Map("uri" -> readAddress))
      val writePodcastKeywordsConfig = WriteConfig(Map("uri" -> writePodcastKeywordAddress))


      // Get all keyword_podcast documents

      val keywordPodcast = MongoSpark.load(sc, readConfig)
        .map(doc => {
          val itunesId = doc.getString("itunesId")
          val keywords = doc.get("keywords").asInstanceOf[java.util.ArrayList[Document]]
            .map(wordDoc => {
              wordDoc.put("itunesId", itunesId)
              wordDoc
            })

          (itunesId, keywords)
        })

      //keywordPodcast.cache()
      var numPodcasts = keywordPodcast.count()


      // Flatten keywords into one large group

      val allKeywords = keywordPodcast
        .flatMap(r => r._2)
        .map(wordDoc => (wordDoc.getString("word"), wordDoc))

      //allKeywords.cache()

      // Calculate idf scores for each word, join resulting rdd back to allKeywords, then construct rdd for keyword_podcast collection

      val keywordPodcastCollectionIntermediate = allKeywords
        .map(r => (r._1, 1))
        .reduceByKey(_+_)
        .map(r => {
            val word = r._1
            val df = r._2

            // idf = log10(N/df) -- N = total number of documents, df = number of documents the term appears in
            val idf = Math.log10(numPodcasts.toDouble / df.toDouble)

            (word, idf)
          })
        .zipWithIndex() // produces unique key for each word

        val totalKeywords = keywordPodcastCollectionIntermediate.count

        val keywordPodcastCollection = keywordPodcastCollectionIntermediate
        .map(r => (r._1._1.asInstanceOf[String], (r._2, r._1._2))) // (word, (unique id, idf))
        .join(allKeywords) // (word,((uniqueId,idf),Document{{itunesId=???, word=???, weight=??, id=-1, tf=??}}))
        .map(r => {
          val word = r._1
          val values = r._2
          val wordId = values._1._1
          val idf = values._1._2
          val intermWordDoc = values._2

          val wordDoc = new Document
          wordDoc.put("id", wordId)
          wordDoc.put("tf", intermWordDoc.getDouble("tf"))
          wordDoc.put("weight", intermWordDoc.getInteger("weight"))
          wordDoc.put("idf", idf)

          (intermWordDoc.getString("itunesId"), wordDoc)
        })
        .groupByKey()
          .map(r => {
            val itunesId = r._1
            val keywords = r._2.asJava

            val keywordPodcastDoc = new Document
            keywordPodcastDoc.put("_id", itunesId)
            keywordPodcastDoc.put("itunesId", itunesId)
            keywordPodcastDoc.put("keywords", keywords)
            keywordPodcastDoc.put("totalKeywords", totalKeywords)

            keywordPodcastDoc
          })

      MongoSpark.save(keywordPodcastCollection, writePodcastKeywordsConfig)

      sc.stop()
    }

  def CalculateUserRecommendations(local: Boolean, userId: Long): Unit ={

    println("Calculate Recommendations for User " + userId)

    var dbAddress = "mongodb://127.0.0.1/podcastdb" // local

    if (!local) {
      dbAddress = "mongodb://client:19dbw964s-27snw224081@104.154.182.252/podcastdb" // remote
    }

    val userSubAddress = dbAddress + ".user_subscription"
    val keywordsAddress = dbAddress + ".keyword_podcast"
    val userRecAddress = dbAddress + ".user_recommendation"

    val spark = SparkSession.builder()
      .master("local")
      .appName("FeedParser")
      .getOrCreate()

    val sc = spark.sparkContext

    val readUserSubConfig = ReadConfig(Map("uri" -> userSubAddress))
    val readKeywordsConfig = ReadConfig(Map("uri" -> keywordsAddress))
    val writeUserRecs = WriteConfig(Map("uri" -> userRecAddress))

    val keywordPodcastDocs = MongoSpark.load(sc, readKeywordsConfig)
    .map(doc => {
      (doc.getString("itunesId"), doc) // (itunesId, keywordPodcastDoc)
    })
      //.cache()

    println("Retreiving user subscription information... ")

    val subs = MongoSpark.load(sc, readUserSubConfig)

    subs.foreach(println)

      val userSubIds = subs
      .filter(doc => doc.getDouble("id").toInt == userId)
      .flatMap(doc => doc.get("subscriptions").asInstanceOf[java.util.ArrayList[String]]).collect()

    println("Calculating subscription vector...")

    println("userSubIds")
    userSubIds.foreach(println)
    val userKeywordPodcastDocs = keywordPodcastDocs
      .filter(r => userSubIds.contains(r._1))
      .cache()


    println("userKeywordPodcastDocs")
    userKeywordPodcastDocs.foreach(println)

    val nDimensions = userKeywordPodcastDocs.collect().toList.get(0)._2.getLong("totalKeywords").toString.toInt
    val vector_float = Array.fill[String](nDimensions)("0.0")


    val userKeywordTfidfsIntermediate = userKeywordPodcastDocs
        .flatMap(r => {
          val keywords = r._2.get("keywords").asInstanceOf[java.util.ArrayList[Document]]
          keywords
        })
        .map(keywordDoc => (keywordDoc.getLong("id"), (keywordDoc.getInteger("weight"), keywordDoc.getInteger("nKeywords"), keywordDoc.getDouble("idf"))))
        .reduceByKey((x, y) => {

          val weight = (x._1 + y._1).asInstanceOf[Integer]

          val result = (weight, x._2, x._3)
          result
        }).cache()

    val nKeywords = userKeywordTfidfsIntermediate.collect().length

    val userKeywordTfidfs = userKeywordTfidfsIntermediate
        .map(r => {
          val id = r._1
          val weight = r._2._1.toDouble
          val idf = r._2._3

          val word = (r._1, (weight.toDouble / nKeywords.toDouble) * idf.toDouble)
          word
        }).collect().toList


    userKeywordTfidfs.foreach(t => {
//      println(t)
      vector_float(t._1.toString.toInt) = t._2.toString
    })

    println("Comparing subscription vector to other podcast vectors using cosine similarity...")

    //code to get podcast details, tf idf from mongodb
    val itunesid = keywordPodcastDocs.map(row => {
      val itunesId = row._1
      val podcastDoc = row._2
      val keywords = podcastDoc.get("keywords").asInstanceOf[java.util.ArrayList[Document]]

      val y = Array.fill[String](nDimensions)("0.0")

      var  vector = ""
      var array1 = Array(0.0)


      keywords.foreach(keyword => {
        if (keyword == null) {
          throw new NoSuchElementException("empty list")

        }
        else {

          val tf = keyword.getDouble("tf")
          val idf = keyword.getDouble("idf")
          val id = keyword.getLong("id").toDouble

          val tf_idf = (tf * idf)

          y(id.toInt) = tf_idf.toString

        }

      })

      //cosine

      var dotProduct = 0.0
      var normA = 0.0
      var normB = 0.0
      var index = y.size - 1

      for (i <- 1 to index) {
        dotProduct += y(i).toFloat * vector_float(i).toFloat
        normA += Math.pow(y(i).toFloat, 2)
        normB += Math.pow(vector_float(i).toFloat, 2)
      }
      val cosine = (dotProduct / (Math.sqrt(normA) * Math.sqrt(normB)))

      (itunesId,cosine)
    })

    val top_cosine: Array[Document] = itunesid.takeOrdered(10)(Ordering[Double].reverse.on(_._2))
      .map(r => {
        val doc = new Document
        doc.put("itunesId", r._1)
        doc.put("similarity", r._2.toString)

        doc
      })

    println("Recommendations: ")
    top_cosine.foreach(println)

    val recommendations = new Document
    recommendations.put("userId", userId)
    recommendations.put("recommendations", top_cosine.toSeq.asJava)
    val rdd = sc.parallelize(Array(recommendations).toSeq)

    println("Saving recommendations to user's account...")
    MongoSpark.save(rdd, writeUserRecs)
  }

  /**
    * MAIN
    *
    * @param args - specify which job to run
    */
  def main(args: Array[String]): Unit = {
    // create Spark context with Spark configuration

    var job = "test"

    if (args.length > 0){
      job = args(0)
    }

    var filePath = "/usr/local/spark-data/feeds.csv"
    if (args.length > 1){
      filePath = args(1)
    }

    println("Running Spark Job:")
    println(job)

    job match {
      case "test" => Test()
      //case "parse-feeds-cluster" => ParseFeedsFromFile(false, args(1))
      case "parse-feeds-local" => ParseFeedsFromFile(true, "/usr/local/spark-data/feeds.csv")
      case "populate-db-feeds-cluster" => PopulateDBWithFeedUrls(false)
      case "populate-db-feeds-local" => PopulateDBWithFeedUrls(true)
      case "process-keywords-local" => ProcessKeywords(true)
      case "process-keywords-cluster" => ProcessKeywords(false)
      case "process-keywords-local-2" => ProcessKeywords2(true)
      case "process-keywords-cluster-2" => ProcessKeywords2(false)
      case "calculate-idfs-local" => CalculateIdfs(true)
      case "calculate-idfs-cluster" => CalculateIdfs(false)
      case "calculate-user-recommendations-local" => CalculateUserRecommendations(true, args(1).toInt)
      case "calculate-user-recommendations-cluster" => CalculateUserRecommendations(true, args(1).toInt)
    }
  }
}

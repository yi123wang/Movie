import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.text.SimpleDateFormat
import java.util.Date

/**
 * @author Daim
 * @date 2024/5/28 15:26
 * @version 1.0
 */
object StatisticsRecommender {
  case class Movie(mid: Int, name: String, descri: String, timelong: String, issue: String,
                   shoot: String, language: String, genres: String, actors: String, directors: String)
  case class Rating(uid: Int, mid: Int, score: Double, timestamp: Int)
  case class MongoConfig(uri:String, db:String)

  /**
   * 推荐电影的结果
   *
   * @param mid   电影推荐的id
   * @param score 电影推荐的评分
   */
  case class Recommendation(mid: Int, score: Double)

  /**
   * 电影类别推荐
   *
   * @param genres 电影类别
   * @param recs   top10的集合
   *               爱情片（8，9  {前10名}）
   */
  case class GenresRecommendation(genres: String, recs: Seq[Recommendation])
  val MOVIE_DATA_PATH = "D:\\IDEA_data\\Moive\\Recommender\\DataLoader\\src\\main\\resources\\movies.csv"
  val RATINGS_DATA_PATH = "D:\\IDEA_data\\Moive\\Recommender\\DataLoader\\src\\main\\resources\\ratings.csv"

  //定义mongoDB的表名
  val MONGODB_MOVIE_COLLECTION = "Movie"
  val MONGODB_RATING_COLLECTION = "Rating"

  // 4种推荐-统计的表的名称；
  //设置一个top的定量值 k 值；
  val MOST_SCORE_OF_NUMBER = 10
  //评分最多
  val RATE_MORE_MOVIES = "RateMoreMovies"
  //近期热门统计
  val RATE_MORE_RECENTLY_MOVIES = "RateMoreRecentlyMovies"
  //平均分
  val AVERAGE_MOVIES = "AverageMovies"
  //类别统计
  val GENRES_TOP_MOVIES = "GenresTopMovies"

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://192.168.8.100:27017/recommender",
      "mongo.db" -> "recommender"
    )
    val sparkConf =  new SparkConf().setMaster(config("spark.cores")).setAppName("StatisticsRecommender")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))
    import spark.implicits._
    val movieDF = spark.
      read.option("uri", mongoConfig.uri)
      .option("collection", MONGODB_MOVIE_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Movie]
      .toDF()

    val ratingDF = spark.
      read.option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Rating]
      .toDF()
    //临时表
    ratingDF.createOrReplaceTempView("ratings")
    //评分最多
    val rateMoreMoviesDF = spark.sql(
      "select mid,count(mid) from ratings group by mid"
    )
    storeDFInMongoDB(rateMoreMoviesDF,RATE_MORE_MOVIES)

    /**
     * 近期热门统计
     * 1.时间状态
     * 2.mid 出现的次数 进行一次top10
     * 3.时间 时间戳：1260759179毫秒
     * 4.时间戳转换成时间格式YYYY-MM-DD HH:MM:SS
     */
    //创建一个时间格式：
    val dateFormat = new SimpleDateFormat("yyyyMM")
    //spark.sqlContext.udf.register("changeDateFormat",(x:String)=>dateFormat.format(new java.util.Date(x.toLong)))
    //将时间戳转换成时间格式 转换成年月
    spark.udf.register("changeDate",
      (x:Int)=>dateFormat.format(new Date(x * 1000)).toInt)
    //数据处理 去掉 uid
    val ratingOfYearMonth = spark.sql("select mid,score,changeDate(timestamp) as yearmonth from ratings")

    ratingOfYearMonth.createOrReplaceTempView("ratingOfMonth")

    //从ratingofMonth这张临时表下查询各个月份的评分和mid count
    val reteMoreRecentlyMoviesDF = spark.sql("select mid,count(mid) as count,yearmonth " +
      "from ratingOfMonth group by yearmonth ,mid " +
      "order by yearmonth desc,count desc")
    storeDFInMongoDB(reteMoreRecentlyMoviesDF,RATE_MORE_RECENTLY_MOVIES)
    //平均分
    val averageMoviesDF = spark.sql(
      "select mid,avg(score) as avg from ratings group by mid"
    )
    storeDFInMongoDB(averageMoviesDF,AVERAGE_MOVIES)

    //统计各类别的top 10
    val genres = List("Action", "Adventure", "Animation", "Comedy", "Crime", "Documentary", "Drama", "Family", "Fantasy", "Foreign", "History", "Horror", "Music", "Mystery"
      , "Romance", "Science", "Tv", "Thriller", "War", "Western")
    //一个电影可能有多个类型？
    val movieWithScore = movieDF.join(averageMoviesDF, "mid")
    val genresRDD = spark.sparkContext.makeRDD(genres)
    val genresTopMoviesDF = genresRDD.cartesian(movieWithScore.rdd)
      .filter{
        //获取电影类型 过滤他的类别
        case (genre,movieRow) => movieRow.getAs[String]("genres").toLowerCase.contains(genre.toLowerCase)
      }.map{
      case (genre,movieRow) =>
        //霹雳火 4.8 【动作 冒险】
        (genre,(movieRow.getAs[Int]("mid"),movieRow.getAs[Double]("avg")))
    }.groupByKey()
      .map{
        case(genre,items) =>
          GenresRecommendation(genre,items.toList.sortWith(
            _._2 > _._2
          ).take(MOST_SCORE_OF_NUMBER).map(x => Recommendation(x._1,x._2)))
      }.toDF()
    storeDFInMongoDB(genresTopMoviesDF,GENRES_TOP_MOVIES)
    spark.stop()

  }
  def storeDFInMongoDB(df:DataFrame,collection_name:String)(implicit mongoConfig:MongoConfig): Unit ={
    df.write
      .option("uri",mongoConfig.uri)
      .option("collection",collection_name)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
  }





}

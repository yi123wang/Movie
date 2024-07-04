import com.mongodb.casbah.Imports.MongoClient
import com.mongodb.casbah.MongoClientURI
import com.mongodb.casbah.commons.MongoDBObject
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.transport.client.PreBuiltTransportClient

import java.net.InetAddress

/**
 * @author Daim
 * @date 2024/5/27 15:52
 * @version 1.0
 */
// 定义样例类
object DataLoader {
  case class Movie(mid: Int, name: String, descri: String, timelong: String, issue: String,
                   shoot: String, language: String, genres: String, actors: String,
                   directors: String)
  case class Rating(uid: Int, mid: Int, score: Double, timestamp: Int)
  case class Tag(uid: Int, mid: Int, tag: String, timestamp: Int)
  case class MongoConfig(uri:String, db:String)
  case class ESConfig(httpHosts:String, transportHosts:String, index:String,
                      clustername:String)
  val MOVIE_DATA_PATH = "D:\\IDEA_data\\Moive\\Recommender\\DataLoader\\src\\main\\resources\\movies.csv"
  val RATING_DATA_PATH = "D:\\IDEA_data\\Moive\\Recommender\\DataLoader\\src\\main\\resources\\ratings.csv"
  val TAG_DATA_PATH = "D:\\IDEA_data\\Moive\\Recommender\\DataLoader\\src\\main\\resources\\tags.csv"


  val MONGODB_MOVIE_COLLECTION = "Movie"
  val MONGODB_RATING_COLLECTION = "Rating"
  val MONGODB_TAG_COLLECTION = "Tag"
  val ES_MOVIE_INDEX = "Movie"

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://192.168.8.100:27017/recommender",
      "mongo.db" -> "recommender",
      "es.httpHosts" -> "192.168.8.100:9200",
      "es.transportHosts" -> "192.168.8.100:9300",
      "es.index" -> "recommender",
      "es.cluster.name" -> "es-cluster"
    )
    /**
     *  学员表：
     *  id  name age  address mobile
     *  10000条
     *  存入mongdb
     *
     *
     *
     *
     */
    val sparkConf =  new SparkConf().setMaster(config("spark.cores")).setAppName("DataLoader")
    // 创建一个 SparkSession
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._
    val movieRDD = spark.sparkContext.textFile(MOVIE_DATA_PATH)
    val movieDF = movieRDD.map(
      item => {
        val attr = item.split("\\^")
        Movie(
          attr(0).toInt,
          attr(1).trim,
          attr(2).trim,
          attr(3).trim,
          attr(4).trim,
          attr(5).trim,
          attr(6).trim,
          attr(7).trim,
          attr(8).trim,
          attr(9).trim
        )
      }
    ).toDF()

    val ratingDF = spark.sparkContext.textFile(RATING_DATA_PATH).map(
      item => {
        val attr = item.split(",")
        Rating(attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt)
      }
    ).toDF()

    val tagDF = spark.sparkContext.textFile(TAG_DATA_PATH).map(
      item => {
        val attr = item.split(",")
        Tag(attr(0).toInt, attr(1).toInt, attr(2).trim, attr(3).toInt)
      }
    ).toDF()

    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))
    storeDataInMongoDB(movieDF, ratingDF, tagDF)


    /**
     * 查询表 链接表
     * es 搜索服务器；
     * 看电影通过什么去搜索
     * 类别/电影名称/演员/年月日/
     * 电影标签/类别
     * 连接查询
     * movie 主表   tag 从表（标签）
     * 左连接
     * 1 狮子王 1999 辛巴 卡梅隆  爱情|励志|动作
     *
     *
     *
     *
     *
     *
     *
     */
    //借助这个包 帮助咱们生成一张左连接的表；
    // 用于ES搜索服务器；
    import org.apache.spark.sql.functions._

    val newTag = tagDF.groupBy($"mid")
      .agg(concat_ws("|", collect_set($"tag"))
        .as("tags"))
      .select("mid", "tags")

    //将处理后的tag数据和movie数据整合；
    // mid  name  descri  timelong  issue  shoot  language  genres  actors  directors  MID TAG 标签
    // 1  龙之吻 卡梅隆   动作片 外语 。。。。。
    val movieWithTagDF = movieDF.join(newTag, Seq("mid", "mid"), "left")
    implicit val esConfig = ESConfig(
      config.get("es.httpHosts").get,
      config.get("es.transportHosts").get,
      config.get("es.index").get,
      config.get("es.cluster.name").get)
    storeDataInES(movieWithTagDF)
  }

  def storeDataInMongoDB(movieDF: DataFrame, ratingDF: DataFrame, tagDF: DataFrame)
                        (implicit mongoConfig: MongoConfig): Unit = {

    val mongoClient = MongoClient(MongoClientURI(mongoConfig.uri))
    mongoClient(mongoConfig.db)(MONGODB_MOVIE_COLLECTION).dropCollection()
    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).dropCollection()
    mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).dropCollection()

    movieDF.write.option("uri", mongoConfig.uri).
      option("collection", MONGODB_MOVIE_COLLECTION).
      mode("overwrite").
      format("com.mongodb.spark.sql").
      save()

    ratingDF.write.option("uri", mongoConfig.uri).
      option("collection", MONGODB_RATING_COLLECTION).
      mode("overwrite").
      format("com.mongodb.spark.sql").
      save()

    tagDF.write.option("uri", mongoConfig.uri).
      option("collection", MONGODB_TAG_COLLECTION).
      mode("overwrite").
      format("com.mongodb.spark.sql").
      save()

    mongoClient(mongoConfig.db)(MONGODB_MOVIE_COLLECTION).
      createIndex(MongoDBObject("mid" -> 1))

    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).
      createIndex(MongoDBObject("uid" -> 1))

    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).
      createIndex(MongoDBObject("mid" -> 1))


    mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).
      createIndex(MongoDBObject("uid" -> 1))

    mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).
      createIndex(MongoDBObject("mid" -> 1))

    mongoClient.close()

  }


    def storeDataInES(movieDF:DataFrame)(implicit eSConfig:ESConfig):Unit ={
    //新建一个ES 配置类：
    val settings:Settings = Settings.builder().
      put("cluster.name",eSConfig.clustername).build()
    //新建一个ES 客户端；集群里面的哪一个节点；
    val esClient = new PreBuiltTransportClient(settings)
    //TransportHost 加进去 正则表达式；
    //localhost:8080 9000 9200 9300
    // 192,168.8.100 统配 所有字符 ： 智能是数字"192.168.8.100:9200"
    val REGEX_HOST_PORT = "(.+):(\\d+)".r
    eSConfig.transportHosts.split(",").foreach {
      case REGEX_HOST_PORT(host:String, port:String) => {
        esClient.addTransportAddress(
          new InetSocketTransportAddress(InetAddress.getByName(host),port.toInt))
      }
    }
    if (esClient.admin().indices().exists(
      new IndicesExistsRequest(eSConfig.index)).actionGet().isExists){
      esClient.admin().indices().delete(new DeleteIndexRequest(eSConfig.index))
    }
    esClient.admin().indices().create(new CreateIndexRequest(eSConfig.index))

    //写入数据到es里；
    movieDF.write.
      option("es.nodes",eSConfig.httpHosts).
      option("es.http.timeout","100m").
      option("es.mapping.id","mid").
      mode("overwrite").
      format("org.elasticsearch.spark.sql").
      save(eSConfig.index+"/"+ES_MOVIE_INDEX)

  }

}

import scalaj.http.{Http,HttpOptions,HttpResponse}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.{Column, DataFrame, SparkSession, Dataset, Row}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scalikejdbc._

//number of activity
val num_str : String = "1"
val num : Int = num_str.toInt
//val filelocation: String = "/Users/nathanstreet/activity"
val actId : String = "8411953649"
def runPost(http: String): String = {
  val responseData =
    """{"client_id":"68868",
      | "client_secret":"c69d2468d636c2a68f89094b57ae96ce93c3ef79",
      | "refresh_token":"e54cd4dbec3711df74baa3e97f35197d77d08c99",
      | "grant_type":"refresh_token"}""".stripMargin
  val request = Http(http)
    .postData(responseData)
    .header("content-type", "application/json")
    .option(HttpOptions.method("POST")).asString

  request.body
}

def getActivityID(accessID: String): String = {
  val response: HttpResponse[String] = Http(s"""https://www.strava.com/api/v3/activities/$actId?access_token=$accessID""").asString
  response.body
}

//empty list to append IDs to
val line : List[Int] = List.range(0, num)
var idArr : Array[Long] = Array()
var actArr : Array[Any] = Array()

val spark: SparkSession = SparkSession.builder()
  .config("spark.master", "local")
  .getOrCreate()

//get access token
val initial_json_response = ujson.read(runPost("https://www.strava.com/oauth/token"))
val access_token = initial_json_response("access_token").str

//det activity IDs
val id_json_response = ujson.read(getActivityID(access_token).stripMargin)
for (n <- line) idArr :+= id_json_response(n)("id").num.toLong

//main activity
val picture_schema = new StructType(Array(
    StructField("id", StringType, true),
    StructField("name", StringType, true),
    StructField("distance", StringType, true),
    StructField("moving_time", StringType, true),
    StructField("elapsed_time", StringType, true),
    StructField("total_elevation_gain", StringType, true),
    StructField("type", StringType, true),
    StructField("sport_type", StringType, true),
    StructField("workout_type", StringType, true),
    StructField("start_date", StringType, true),
    StructField("start_date_local", StringType, true),
    StructField("map", BooleanType, true),
    StructField("average_speed", StringType, true),
    StructField("load_ts", DateType, true)))

//create Empty Table
var activityDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], picture_schema)

//convert activity JSON to DF
def appendActivityRows(activity: String): Unit = {
    import spark.implicits._
    val value = Seq(activity).toDF()
    val actDf = value
      .withColumn("value",from_json(col("value"),picture_schema))
      .select(col("value.*"))
      .withColumn("dt", current_date())
    activityDF = activityDF.union(actDf)
}

appendActivityRows(activity: String)
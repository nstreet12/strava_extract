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
val filelocation: String = "/Users/nathanstreet/activity"

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
  val response: HttpResponse[String] = Http(s"""https://www.strava.com/api/v3/athlete/activities?page=1&per_page=$num_str&access_token=$accessID""").asString
  response.body
}

def getActivityData(accessID: String, activityID: Long): String = {
  val activityID_str = activityID.toString
  val response: HttpResponse[String] = Http(s"""https://www.strava.com/api/v3/activities/$activityID_str?access_token=$accessID""").asString
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

//pull activity data
for (id <- idArr) actArr :+= ujson.read(getActivityData(access_token,id).stripMargin)

actArr

//main activity
val activity_schema = new StructType(Array(
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
    StructField("timezone", StringType, true),
    StructField("utc_offset", StringType, true),
    StructField("location_city", StringType, true),
    StructField("location_state", StringType, true),
    StructField("location_country", StringType, true),
    StructField("achievement_count", StringType, true),
    StructField("kudos_count", StringType, true),
    StructField("comment_count", StringType, true),
    StructField("athlete_count", StringType, true),
    StructField("photo_count", StringType, true),
    StructField("map", StringType, true),
    StructField("trainer", BooleanType, true),
    StructField("commute", BooleanType, true),
    StructField("manual", BooleanType, true),
    StructField("private", BooleanType, true),
    StructField("visibility", StringType, true),
    StructField("flagged", BooleanType, true),
    StructField("gear_id", StringType, true),
    StructField("start_latlng", StringType, true),
    StructField("end_latlng", StringType, true),
    StructField("average_speed", StringType, true),
    StructField("max_speed", StringType, true),
    StructField("has_heartrate", BooleanType, true),
    StructField("heartrate_opt_out", BooleanType, true),
    StructField("display_hide_heartrate_option", BooleanType, true),
    StructField("elev_high", StringType, true),
    StructField("elev_low", StringType, true),
    StructField("upload_id", StringType, true),
    StructField("upload_id_str", StringType, true),
    StructField("external_id", StringType, true),
    StructField("from_accepted_tag", BooleanType, true),
    StructField("pr_count", BooleanType, true),
    StructField("total_photo_count", StringType, true),
    StructField("has_kudoed", BooleanType, true),
    StructField("description", StringType, true),
    StructField("calories", StringType, true),
    StructField("perceived_exertion", StringType, true),
    StructField("prefer_perceived_exertion", StringType, true),
    StructField("segment_efforts", StringType, true),
    StructField("splits_metric", StringType, true),
    StructField("splits_standard", StringType, true),
    StructField("laps", StringType, true),
    StructField("gear", StringType, true),
    StructField("photos", StringType, true),
    StructField("stats_visibility", StringType, true),
    StructField("hide_from_home", BooleanType, true),
    StructField("device_name", StringType, true),
    StructField("embed_token", StringType, true),
    StructField("similar_activities", StringType, true),
    StructField("available_zones", StringType, true),
    StructField("dt", DateType, true)))

//val segment_eff_schema = new StructType(Array(
//    StructField("id", IntegerType, true),
//    StructField("resource_state", IntegerType, true),
//    StructField("name", StringType, true),
//    StructField("activity", StringType, true),
//    StructField("athlete", StringType, true),
//    StructField("elapsed_time", IntegerType, true),
//    StructField("moving_time", IntegerType, true),
//    StructField("start_date", StringType, true),
//    StructField("start_date_local", StringType, true),
//    StructField("distance", FloatType, true),
//    StructField("start_index", IntegerType, true),
//    StructField("end_index", IntegerType, true),
//    StructField("average_cadence", FloatType, true),
//    StructField("device_watts", BooleanType, true),
//    StructField("average_heartrate", FloatType, true),
//    StructField("max_heartrate", FloatType, true),
//    StructField("segment", StringType, true),
//    StructField("pr_rank", StringType, true),
//    StructField("achievements", StringType, true),
//    StructField("kom_rank", StringType, true),
//    StructField("hidden", BooleanType, true)))

val map_schema = new StructType(Array(
    StructField("id", StringType, true),
    StructField("polyline", StringType, true),
    StructField("resource_state", IntegerType, true),
    StructField("summary_polyline", StringType, true)))

val gear_schema = new StructType(Array(
    StructField("id", StringType, true),
    StructField("primary", BooleanType, true),
    StructField("name", StringType, true),
    StructField("nickname", StringType, true),
    StructField("resource_state", StringType, true),
    StructField("retired", BooleanType, true),
    StructField("distance", IntegerType, true),
    StructField("converted_distance", FloatType, true)))

//create Empty Table
var activityDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], activity_schema)
//var mapDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], map_schema)

//convert activity JSON to DF
def appendActivityRows(activity: String): Unit = {
    import spark.implicits._
    val value = Seq(activity).toDF()
    val actDf = value
      .withColumn("value",from_json(col("value"),activity_schema))
      .select(col("value.*"))
      .withColumn("dt", current_date())
    activityDF = activityDF.union(actDf)
}

for (act <- actArr) appendActivityRows(act.toString)
activityDF.show(true)

//MAP
val mapData = activityDF
  .withColumn("value", from_json(col("map"), map_schema))
  .select(col("id").as("actId"),col("value.*"))
  .withColumn("dt", current_timestamp())
//mapDF = mapDF.union(mapData)

mapData.show(true)

//GEAR
val gearData = activityDF
  .withColumn("value", from_json(col("gear"), gear_schema))
  .select(col("id").as("actId"),col("value.*"))
  .withColumn("dt", current_timestamp())
//mapDF = mapDF.union(mapData)

gearData.show(true)

//activityDF
//  .repartition(1)
//  .write
//  .partitionBy("dt")
//  .option("header", true)
//  .mode("append")
//  .format("parquet")
//  .save(filelocation)
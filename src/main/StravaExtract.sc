import scalaj.http.{Http,HttpOptions,HttpResponse}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SparkSession, Row, SaveMode}
import java.sql.DriverManager

// number of activity
val num : Int = 20
val start_date : String = "start_date"

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
  val response: HttpResponse[String] = Http(s"""https://www.strava.com/api/v3/athlete/activities?page=1&per_page=20&access_token=$accessID&order_by=$start_date""").asString
  response.body
}

def getActivityData(accessID: String, activityID: Long): String = {
    val activityID_str : String = activityID.toString
    val response: HttpResponse[String] = Http(s"""https://www.strava.com/api/v3/activities/$activityID_str?access_token=$accessID""").asString
    response.body
}

// empty list to append IDs to
val line : List[Int] = List.range(0, num)
var idArr : Array[Long] = Array()
var actArr : Array[Any] = Array()

val spark: SparkSession = SparkSession.builder()
  .config("spark.master", "local")
  .getOrCreate()

// get access token
val initial_json_response = ujson.read(runPost("https://www.strava.com/oauth/token"))
val access_token = initial_json_response("access_token").str

// detail activity IDs
val id_json_response = ujson.read(getActivityID(access_token).stripMargin)
for (n <- line) idArr :+= id_json_response(n)("id").num.toLong

// pull activity data
for (id <- idArr) actArr :+= ujson.read(getActivityData(access_token,id).stripMargin)

actArr

// activity
val activity_schema = new StructType(Array(
    StructField("resource_state", IntegerType, true),
    StructField("athlete", StringType, true),
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
    StructField("upload_ts", TimestampType, true)))

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
      .withColumn("upload_ts", current_timestamp())
    activityDF = activityDF.union(actDf)
}

for (act <- actArr) appendActivityRows(act.toString)
activityDF.show(true)

//activityDF
//  .repartition(1)
//  .write
//  .partitionBy("dt")
//  .option("header", true)
//  .mode("append")
//  .format("parquet")
//  .save(filelocation)

// Define the JDBC connection properties
val dbHost = "192.168.0.5"
val dbName = "homeassistant"
val dbPort = "3306"
val dbUsername = "homeassistant"
val dbPassword = "NSStennis123!!!"

val dbUrl = s"jdbc:mysql://$dbHost:$dbPort/$dbName"

val connectionProperties = new java.util.Properties()
connectionProperties.setProperty("user", dbUsername)
connectionProperties.setProperty("password", dbPassword)

// Define the table name
val tableName = "activity_detail_bronze"

// Create the table if it doesn't exist
val createTableQuery =
    s"""
       |CREATE TABLE IF NOT EXISTS $tableName (
       |  resource_state INT,
       |  athlete TEXT,
       |  id TEXT,
       |  name TEXT,
       |  distance TEXT,
       |  moving_time TEXT,
       |  elapsed_time TEXT,
       |  total_elevation_gain TEXT,
       |  type TEXT,
       |  sport_type TEXT,
       |  workout_type TEXT,
       |  start_date TEXT,
       |  start_date_local TEXT,
       |  timezone TEXT,
       |  utc_offset TEXT,
       |  location_city TEXT,
       |  location_state TEXT,
       |  location_country TEXT,
       |  achievement_count TEXT,
       |  kudos_count TEXT,
       |  comment_count TEXT,
       |  athlete_count TEXT,
       |  photo_count TEXT,
       |  map TEXT,
       |  trainer BOOLEAN,
       |  commute BOOLEAN,
       |  manual BOOLEAN,
       |  private BOOLEAN,
       |  visibility TEXT,
       |  flagged BOOLEAN,
       |  gear_id TEXT,
       |  start_latlng TEXT,
       |  end_latlng TEXT,
       |  average_speed TEXT,
       |  max_speed TEXT,
       |  has_heartrate BOOLEAN,
       |  heartrate_opt_out BOOLEAN,
       |  display_hide_heartrate_option BOOLEAN,
       |  elev_high TEXT,
       |  elev_low TEXT,
       |  upload_id TEXT,
       |  upload_id_str TEXT,
       |  external_id TEXT,
       |  from_accepted_tag BOOLEAN,
       |  pr_count BOOLEAN,
       |  total_photo_count TEXT,
       |  has_kudoed BOOLEAN,
       |  description TEXT,
       |  calories TEXT,
       |  perceived_exertion TEXT,
       |  prefer_perceived_exertion TEXT,
       |  segment_efforts TEXT,
       |  splits_metric TEXT,
       |  splits_standard TEXT,
       |  laps TEXT,
       |  gear TEXT,
       |  photos TEXT,
       |  stats_visibility TEXT,
       |  hide_from_home BOOLEAN,
       |  device_name TEXT,
       |  embed_token TEXT,
       |  similar_activities TEXT,
       |  available_zones TEXT,
       |  upload_ts TIMESTAMP
       |)
       |""".stripMargin


import java.sql.DriverManager
Class.forName("org.mariadb.jdbc.Driver")

val conn = DriverManager.getConnection(dbUrl, dbUsername, dbPassword)
val stmt = conn.createStatement()
stmt.executeUpdate(createTableQuery)

// Write the DataFrame to the MariaDB table
activityDF.write
  .mode(SaveMode.Append)
  .jdbc(dbUrl, tableName, connectionProperties)

// Close the JDBC connection
stmt.close()
conn.close()
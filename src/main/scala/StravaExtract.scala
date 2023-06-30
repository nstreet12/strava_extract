import scalaj.http.{Http, HttpOptions, HttpResponse}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import java.sql.DriverManager

object StravaExtract {
  // number of activity
  val num: Int = 10
  val start_date: String = "start_date"

  def runPost(http: String): String = {
    // Logging: Sending POST request to Strava API
    println("Sending POST request to Strava API...")
    val client_id: String = Creds.client_id
    val client_secret: String = Creds.client_secret
    val refresh_token: String = Creds.refresh_token
    val grant_type: String = Creds.grant_type

    val responseData =
      s"""{"client_id":"$client_id",
         | "client_secret":"$client_secret",
         | "refresh_token":"$refresh_token",
         | "grant_type":"$grant_type"}""".stripMargin
    println(responseData)
    val request = Http(http)
      .postData(responseData)
      .header("content-type", "application/json")
      .option(HttpOptions.method("POST")).asString
    println(request)
    request.body
  }

  def getActivityID(accessID: String): String = {
    // Logging: Sending GET request to Strava API for activity IDs
    println("Sending GET request to Strava API for activity IDs...")
    println(accessID)
    val response: HttpResponse[String] = Http(s"""https://www.strava.com/api/v3/athlete/activities?page=1&per_page=$num&access_token=$accessID&order_by=$start_date""").asString
    println(response)
    response.body
  }

  def getActivityData(accessID: String, activityID: Long): String = {
    // Logging: Sending GET request to Strava API for activity data
    println(s"Sending GET request to Strava API for activity $activityID...")
    val activityID_str: String = activityID.toString
    val path = s"""https://www.strava.com/api/v3/activities/$activityID_str?access_token=$accessID"""
    val response: HttpResponse[String] = Http(path).asString
    response.body
  }

  def main(args: Array[String]): Unit = {

    val connectionProperties = new java.util.Properties()
    connectionProperties.setProperty("user", Creds.dbUsername)
    connectionProperties.setProperty("password", Creds.dbPassword)

    // Create the SparkSession
    val spark = SparkSession.builder()
      .appName("Spark Application")
      .config("spark.master", "local")
      .getOrCreate()

    // Empty list to append IDs to
    val line: List[Int] = List.range(0, num)
    var idArr: Array[Long] = Array()
    var actArr: Array[Any] = Array()

    // Get access token
    // Logging: Running POST request to retrieve access token
    println("Retrieving access token...")
    val initial_json_response = ujson.read(runPost(Path.stravaPostURL))
    val access_token = initial_json_response("access_token").str

    // Detail activity IDs
    // Logging: Running GET request to retrieve activity IDs
    println("Retrieving activity IDs...")
    val id_json_response = ujson.read(getActivityID(access_token).stripMargin)
    for (n <- line) idArr :+= id_json_response(n)("id").num.toLong
    // println(idArr)

    // Pull activity data
    // Logging: Retrieving activity data for each ID
    println("Retrieving activity data...")
    for (id <- idArr) actArr :+= ujson.read(getActivityData(access_token, id).stripMargin)

    // Create an empty DataFrame
    var activityDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], Schema.activity_schema)

    // Convert activity JSON to DataFrame
    def appendActivityRows(activity: String): Unit = {
      import spark.implicits._
      val value = Seq(activity).toDF()
      val log_act = activity.take(100)
      val actDf = value
        .withColumn("value", from_json(col("value"), Schema.activity_schema))
        .select(col("value.*"))
        .withColumn("upload_ts", current_timestamp())
      println(s"""Appending activity...$log_act""")
      activityDF = activityDF.union(actDf)
    }

    for (act <- actArr) appendActivityRows(act.toString)
    activityDF.show(true)

    // Create a JDBC connection
    Class.forName("org.mariadb.jdbc.Driver")
    println("Creating JDBC connection...")
//    println(Path.dbURL)
    val conn = DriverManager.getConnection(Path.dbURL, Creds.dbUsername, Creds.dbPassword)
    val stmt = conn.createStatement()
    stmt.executeUpdate(Schema.createTableQuery)

    // Write the DataFrame to the MariaDB table
    // Logging: Writing DataFrame to MariaDB table
    println("Writing DataFrame to MariaDB table...")
    activityDF.write
      .mode(SaveMode.Append)
      .jdbc(Path.dbURL, Schema.tableName, connectionProperties)

    // Close the JDBC connection
    stmt.close()
    conn.close()

    // Stop the SparkSession
    spark.stop()
  }
}

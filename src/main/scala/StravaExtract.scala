import scalaj.http.{Http, HttpOptions, HttpResponse}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import java.sql.DriverManager

object StravaExtract {
  // number of activity
  val num: Int = 20
  val start_date: String = "start_date"
  val stravaGET: String = Path.stravaGetURL
  val stravaPOST: String = Path.stravaPostURL

  def runPost(http: String): String = {

    val client_id = Creds.client_id
    val client_secret = Creds.client_secret
    val refresh_token = Creds.refresh_token
    val grant_type = Creds.grant_type

    val responseData =
      s"""{"client_secret":$client_id,
        | "client_secret":$client_secret,
        | "refresh_token":$refresh_token,
        | "grant_type":$grant_type}""".stripMargin
    val request = Http(http)
      .postData(responseData)
      .header("content-type", "application/json")
      .option(HttpOptions.method("POST")).asString

    request.body
  }

  def getActivityID(accessID: String): String = {
    val response: HttpResponse[String] = Http(s"""$stravaGET/athlete/activities?page=1&per_page=$num&access_token=$accessID&order_by=$start_date""").asString
    response.body
  }

  def getActivityData(accessID: String, activityID: Long): String = {
    val activityID_str: String = activityID.toString
    val response: HttpResponse[String] = Http(s"""$stravaGET/activities/$activityID_str?access_token=$accessID""").asString
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
    val initial_json_response = ujson.read(runPost(stravaPOST))
    val access_token = initial_json_response("access_token").str

    // Detail activity IDs
    val id_json_response = ujson.read(getActivityID(access_token).stripMargin)
    for (n <- line) idArr :+= id_json_response(n)("id").num.toLong

    // Pull activity data
    for (id <- idArr) actArr :+= ujson.read(getActivityData(access_token, id).stripMargin)

    // Create an empty DataFrame
    var activityDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], Schema.activity_schema)

    // Convert activity JSON to DataFrame
    def appendActivityRows(activity: String): Unit = {
      import spark.implicits._
      val value = Seq(activity).toDF()
      val actDf = value
        .withColumn("value", from_json(col("value"), Schema.activity_schema))
        .select(col("value.*"))
        .withColumn("upload_ts", current_timestamp())
      activityDF = activityDF.union(actDf)
    }

    for (act <- actArr) appendActivityRows(act.toString)
    activityDF.show(true)

    // Create a JDBC connection
    Class.forName("org.mariadb.jdbc.Driver")
    val conn = DriverManager.getConnection(Path.dbURL, Creds.dbUsername, Creds.dbPassword)
    val stmt = conn.createStatement()
    stmt.executeUpdate(Schema.createTableQuery)

    // Write the DataFrame to the MariaDB table
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

import StravaExtract.start_date

object Path {
  val stravaPostURL = "https://www.strava.com/oauth/token"
  val dbURL = s"jdbc:mysql://${Creds.dbHost}:${Creds.dbPort}/${Creds.dbName}"
}

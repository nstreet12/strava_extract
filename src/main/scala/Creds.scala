import com.typesafe.config.ConfigFactory

object Creds {
  private val config = ConfigFactory.load()

  val dbHost: String = config.getString("credentials.dbHost")
  val dbName: String = config.getString("credentials.dbName")
  val dbPort: String = config.getString("credentials.dbPort")
  val dbUsername: String = config.getString("credentials.dbUsername")
  val dbPassword: String = config.getString("credentials.dbPassword")
  val client_id: String = config.getString("credentials.client_id")
  val client_secret: String = config.getString("credentials.client_secret")
  val refresh_token: String = config.getString("credentials.refresh_token")
  val grant_type: String = config.getString("credentials.grant_type")
}

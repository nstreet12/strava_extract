import kiambogo.scrava.ScravaClient
import kiambogo.scrava.models._
import net.liftweb.json._


val testToken = "69ce1bdca765ebf26af18642003cdbe7350c68da"
val testClient = new ScravaClient(testToken)

val act = testClient.listAthleteActivities()

actList : List = act(2)
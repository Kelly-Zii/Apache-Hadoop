import org.scalacheck._
import Prop._
import Test._

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

class TestSolution extends AnyFunSuite {

  // Mock the Kafka producer by providing a list of generated events
  def mockGenerateBeeFlightEvents(events: List[String]): Unit = {
    events.foreach { event =>
      GenerateBeeFlight.sendLandingEvent(???)
    }
  }

  // Mock the Kafka consumer by providing a list of bee counts
  def mockCountBeeLandings(events: List[String]): Map[String, Long] = {
    val beeCounts = CountBeeLandings.countBeeLandings(???)
    beeCounts.toMap
  }

  test("Bee count should match expected count") {
    // Replace these values with your own simple records
    val generatedEvents = List(
      "1,1,1,1",
      "2,1,1,1",
      "3,1,1,1",
      "4,1,2,2"
    )

    // Replace these values with your own expected counts
    val expectedBeeCounts = Map(
      "1,1" -> 3L,
      "2,2" -> 1L
    )

    mockGenerateBeeFlightEvents(generatedEvents)
    val actualBeeCounts = mockCountBeeLandings(generatedEvents)

    actualBeeCounts shouldEqual expectedBeeCounts
  }
}

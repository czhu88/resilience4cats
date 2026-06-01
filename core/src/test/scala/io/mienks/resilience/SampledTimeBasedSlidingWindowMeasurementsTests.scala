package io.mienks.resilience

import cats.effect.IO
import cats.effect.testkit.TestControl
import cats.syntax.all._
import Measurements.Snapshot
import munit.CatsEffectSuite

import scala.concurrent.duration._

class SampledTimeBasedSlidingWindowMeasurementsTests extends CatsEffectSuite {

  private def doSnap(minNumberOfCalls: Int)(totalMeasurements: Int, totalFailures: Int): Snapshot =
    Snapshot(
      totalMeasurements = totalMeasurements,
      totalFailures = totalFailures,
      isInitialized = totalMeasurements >= minNumberOfCalls
    )

  test("SampledTimeBasedSlidingWindowMeasurements with all successes") {
    val minNumberOfCalls                                           = 1
    def snap(totalMeasurements: Int, totalFailures: Int): Snapshot =
      doSnap(minNumberOfCalls)(totalMeasurements, totalFailures)
    TestControl.executeEmbed {
      for {
        measurements <- SampledTimeBasedSlidingWindowMeasurements[IO](
          numberOfBuckets = 5,
          bucketSize = 1.second,
          minNumberOfCalls = minNumberOfCalls
        )
        _         <- measurements.recordSuccess.replicateA_(5)
        snapshot1 <- measurements.sample
        _         <- IO.sleep(1.second) >> measurements.recordSuccess.replicateA_(5)
        snapshot2 <- measurements.sample
        _         <- IO.sleep(4.seconds)
        snapshot3 <- measurements.sample
      } yield {
        assertEquals(
          List(snapshot1, snapshot2, snapshot3),
          List(
            snap(totalMeasurements = 5, totalFailures = 0),
            snap(totalMeasurements = 10, totalFailures = 0),
            snap(totalMeasurements = 5, totalFailures = 0)
          )
        )
      }
    }
  }

  test("SampledTimeBasedSlidingWindowMeasurements with failures and successes") {
    val minNumberOfCalls                                           = 1
    def snap(totalMeasurements: Int, totalFailures: Int): Snapshot =
      doSnap(minNumberOfCalls)(totalMeasurements, totalFailures)
    TestControl.executeEmbed {
      for {
        measurements <- SampledTimeBasedSlidingWindowMeasurements[IO](
          numberOfBuckets = 3,
          bucketSize = 1.second,
          minNumberOfCalls = minNumberOfCalls
        )
        _         <- measurements.recordSuccess.replicateA_(2) >> measurements.recordFailure.replicateA_(2)
        snapshot1 <- measurements.sample
        _         <- IO.sleep(1.second) >> measurements.recordFailure.replicateA_(3)
        snapshot2 <- measurements.sample
        _         <- IO.sleep(2.seconds)
        snapshot3 <- measurements.sample
      } yield {
        assertEquals(
          List(snapshot1, snapshot2, snapshot3),
          List(
            snap(totalMeasurements = 4, totalFailures = 2),
            snap(totalMeasurements = 7, totalFailures = 5),
            snap(totalMeasurements = 3, totalFailures = 3)
          )
        )
      }
    }
  }

  test("snapshot isInitialized reflects minNumberOfCalls") {
    val minNumberOfCalls                                           = 3
    def snap(totalMeasurements: Int, totalFailures: Int): Snapshot =
      doSnap(minNumberOfCalls)(totalMeasurements, totalFailures)
    TestControl.executeEmbed {
      for {
        measurements <- SampledTimeBasedSlidingWindowMeasurements[IO](
          numberOfBuckets = 3,
          bucketSize = 1.second,
          minNumberOfCalls = minNumberOfCalls
        )
        _  <- measurements.recordSuccess
        s1 <- measurements.sample
        _ = assertEquals(s1, snap(totalMeasurements = 1, totalFailures = 0))
        _  <- measurements.recordFailure
        s2 <- measurements.sample
        _ = assertEquals(s2, snap(totalMeasurements = 2, totalFailures = 1))
        _  <- measurements.recordSuccess
        s3 <- measurements.sample
        _ = assertEquals(s3, snap(totalMeasurements = 3, totalFailures = 1))
      } yield ()
    }
  }

  test("concurrent recordings are included in the next sample") {
    for {
      measurements <- SampledTimeBasedSlidingWindowMeasurements[IO](
        numberOfBuckets = 3,
        bucketSize = 1.second,
        minNumberOfCalls = 1
      )
      _        <- measurements.recordSuccess.parReplicateA_(100)
      _        <- measurements.recordFailure.parReplicateA_(50)
      snapshot <- measurements.sample
    } yield assertEquals(
      snapshot,
      Snapshot(totalMeasurements = 150, totalFailures = 50, isInitialized = true)
    )
  }
}

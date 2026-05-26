package io.mienks.resilience

import cats.effect.IO
import Measurements.Snapshot
import munit.CatsEffectSuite

class CountBasedSlidingWindowMeasurementsTests extends CatsEffectSuite {

  private def doSnap(minNumberOfCalls: Int)(totalMeasurements: Int, totalFailures: Int): Snapshot =
    Snapshot(
      totalMeasurements = totalMeasurements,
      totalFailures = totalFailures,
      isInitialized = totalMeasurements >= minNumberOfCalls
    )

  test("CountBasedSlidingWindowMeasurements with all successes") {
    val minNumberOfCalls                                           = 1
    def snap(totalMeasurements: Int, totalFailures: Int): Snapshot =
      doSnap(minNumberOfCalls)(totalMeasurements, totalFailures)
    for {
      measurements <- CountBasedSlidingWindowMeasurements[IO](windowSize = 5, minNumberOfCalls = minNumberOfCalls)
      snapshots    <- measurements.record(isFailure = false).replicateA(10)
    } yield {
      assertEquals(
        snapshots,
        List(
          snap(totalMeasurements = 1, totalFailures = 0),
          snap(totalMeasurements = 2, totalFailures = 0),
          snap(totalMeasurements = 3, totalFailures = 0),
          snap(totalMeasurements = 4, totalFailures = 0),
          snap(totalMeasurements = 5, totalFailures = 0),
          snap(totalMeasurements = 5, totalFailures = 0), // window wraps around
          snap(totalMeasurements = 5, totalFailures = 0),
          snap(totalMeasurements = 5, totalFailures = 0),
          snap(totalMeasurements = 5, totalFailures = 0),
          snap(totalMeasurements = 5, totalFailures = 0)
        )
      )
    }
  }

  test("CountBasedSlidingWindowMeasurements with all failures") {
    val minNumberOfCalls                                           = 1
    def snap(totalMeasurements: Int, totalFailures: Int): Snapshot =
      doSnap(minNumberOfCalls)(totalMeasurements, totalFailures)
    for {
      measurements <- CountBasedSlidingWindowMeasurements[IO](windowSize = 5, minNumberOfCalls = minNumberOfCalls)
      snapshots    <- measurements.record(isFailure = true).replicateA(10)
    } yield {
      assertEquals(
        snapshots,
        List(
          snap(totalMeasurements = 1, totalFailures = 1),
          snap(totalMeasurements = 2, totalFailures = 2),
          snap(totalMeasurements = 3, totalFailures = 3),
          snap(totalMeasurements = 4, totalFailures = 4),
          snap(totalMeasurements = 5, totalFailures = 5),
          snap(totalMeasurements = 5, totalFailures = 5), // window wraps around
          snap(totalMeasurements = 5, totalFailures = 5),
          snap(totalMeasurements = 5, totalFailures = 5),
          snap(totalMeasurements = 5, totalFailures = 5),
          snap(totalMeasurements = 5, totalFailures = 5)
        )
      )
    }
  }

  test("CountBasedSlidingWindowMeasurements with steady failures") {
    val minNumberOfCalls                                           = 1
    def snap(totalMeasurements: Int, totalFailures: Int): Snapshot =
      doSnap(minNumberOfCalls)(totalMeasurements, totalFailures)
    for {
      measurements <- CountBasedSlidingWindowMeasurements[IO](windowSize = 6, minNumberOfCalls = minNumberOfCalls)
      action = measurements.record(isFailure = false).flatMap { snapshot1 =>
        measurements.record(isFailure = true).map(snapshot2 => List(snapshot1, snapshot2))
      }
      snapshots <- action.replicateA(6).map(_.flatten)
    } yield {
      assertEquals(
        snapshots,
        List(
          snap(totalMeasurements = 1, totalFailures = 0),
          snap(totalMeasurements = 2, totalFailures = 1),
          snap(totalMeasurements = 3, totalFailures = 1),
          snap(totalMeasurements = 4, totalFailures = 2),
          snap(totalMeasurements = 5, totalFailures = 2),
          snap(totalMeasurements = 6, totalFailures = 3),
          snap(totalMeasurements = 6, totalFailures = 3), // window wraps around
          snap(totalMeasurements = 6, totalFailures = 3),
          snap(totalMeasurements = 6, totalFailures = 3),
          snap(totalMeasurements = 6, totalFailures = 3),
          snap(totalMeasurements = 6, totalFailures = 3),
          snap(totalMeasurements = 6, totalFailures = 3)
        )
      )
    }
  }

  test("CountBasedSlidingWindowMeasurements with sudden failures") {
    val minNumberOfCalls                                           = 1
    def snap(totalMeasurements: Int, totalFailures: Int): Snapshot =
      doSnap(minNumberOfCalls)(totalMeasurements, totalFailures)
    for {
      measurements <- CountBasedSlidingWindowMeasurements[IO](windowSize = 6, minNumberOfCalls = minNumberOfCalls)
      _            <- measurements.record(isFailure = false).replicateA(9)
      ss1          <- measurements.record(isFailure = true)
      ss2          <- measurements.record(isFailure = true)
      ss3          <- measurements.record(isFailure = true)
      ss4          <- measurements.record(isFailure = false)
    } yield {
      assertEquals(
        List(ss1, ss2, ss3, ss4),
        List(
          snap(totalMeasurements = 6, totalFailures = 1),
          snap(totalMeasurements = 6, totalFailures = 2),
          snap(totalMeasurements = 6, totalFailures = 3),
          snap(totalMeasurements = 6, totalFailures = 3)
        )
      )
    }
  }

  test("snapshot isInitialized reflects minNumberOfCalls") {
    val minNumberOfCalls                                           = 3
    def snap(totalMeasurements: Int, totalFailures: Int): Snapshot =
      doSnap(minNumberOfCalls)(totalMeasurements, totalFailures)
    for {
      measurements <- CountBasedSlidingWindowMeasurements[IO](windowSize = 5, minNumberOfCalls = minNumberOfCalls)
      s1           <- measurements.record(isFailure = false)
      _ = assertEquals(s1, snap(totalMeasurements = 1, totalFailures = 0))
      s2 <- measurements.record(isFailure = false)
      _ = assertEquals(s2, snap(totalMeasurements = 2, totalFailures = 0))
      s3 <- measurements.record(isFailure = false)
      _ = assertEquals(s3, snap(totalMeasurements = 3, totalFailures = 0))
      _  <- measurements.reset
      s4 <- measurements.record(isFailure = false)
      _ = assertEquals(s4, snap(totalMeasurements = 1, totalFailures = 0))
    } yield ()
  }

  test("recordings after reset produce correct snapshots") {
    val minNumberOfCalls                                           = 1
    def snap(totalMeasurements: Int, totalFailures: Int): Snapshot =
      doSnap(minNumberOfCalls)(totalMeasurements, totalFailures)
    for {
      measurements <- CountBasedSlidingWindowMeasurements[IO](windowSize = 3, minNumberOfCalls = minNumberOfCalls)
      _            <- measurements.record(isFailure = true).replicateA(3)
      snapshot     <- measurements.record(isFailure = true)
      _ = assertEquals(snapshot, snap(totalMeasurements = 3, totalFailures = 3))

      _ <- measurements.reset

      s1 <- measurements.record(isFailure = false)
      _ = assertEquals(s1, snap(totalMeasurements = 1, totalFailures = 0))
      s2 <- measurements.record(isFailure = true)
      _ = assertEquals(s2, snap(totalMeasurements = 2, totalFailures = 1))
      s3 <- measurements.record(isFailure = false)
      _ = assertEquals(s3, snap(totalMeasurements = 3, totalFailures = 1))
      s4 <- measurements.record(isFailure = true)
      _ = assertEquals(s4, snap(totalMeasurements = 3, totalFailures = 2))
    } yield ()
  }

  test("CountBasedSlidingWindowMeasurements with windowSize = 1") {
    val minNumberOfCalls                                           = 1
    def snap(totalMeasurements: Int, totalFailures: Int): Snapshot =
      doSnap(minNumberOfCalls)(totalMeasurements, totalFailures)
    for {
      measurements <- CountBasedSlidingWindowMeasurements[IO](windowSize = 1, minNumberOfCalls = minNumberOfCalls)
      s1           <- measurements.record(isFailure = false)
      _ = assertEquals(s1, snap(totalMeasurements = 1, totalFailures = 0))
      s2 <- measurements.record(isFailure = true)
      _ = assertEquals(s2, snap(totalMeasurements = 1, totalFailures = 1))
      s3 <- measurements.record(isFailure = false)
      _ = assertEquals(s3, snap(totalMeasurements = 1, totalFailures = 0))
      s4 <- measurements.record(isFailure = true)
      _ = assertEquals(s4, snap(totalMeasurements = 1, totalFailures = 1))
      s5 <- measurements.record(isFailure = true)
      _ = assertEquals(s5, snap(totalMeasurements = 1, totalFailures = 1))
    } yield ()
  }
}

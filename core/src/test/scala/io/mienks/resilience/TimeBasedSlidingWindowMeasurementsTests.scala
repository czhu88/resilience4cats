package io.mienks.resilience

import cats.effect.IO
import cats.effect.testkit.TestControl
import Measurements.Snapshot
import munit.CatsEffectSuite

import scala.concurrent.duration._

class TimeBasedSlidingWindowMeasurementsTests extends CatsEffectSuite {

  private def doSnap(minNumberOfCalls: Int)(totalMeasurements: Int, totalFailures: Int): Snapshot =
    Snapshot(
      totalMeasurements = totalMeasurements,
      totalFailures = totalFailures,
      isInitialized = totalMeasurements >= minNumberOfCalls
    )

  test("TimeBasedSlidingWindowMeasurements with all successes") {
    val minNumberOfCalls                                           = 1
    def snap(totalMeasurements: Int, totalFailures: Int): Snapshot =
      doSnap(minNumberOfCalls)(totalMeasurements, totalFailures)
    TestControl.executeEmbed {
      for {
        measurements <- TimeBasedSlidingWindowMeasurements[IO](
          numberOfBuckets = 5,
          bucketSize = 1.second,
          minNumberOfCalls = minNumberOfCalls
        )
        record = measurements.record(isFailure = false)
        snapshots1 <- record.replicateA(5)
        snapshots2 <- (IO.sleep(1.second) >> record).replicateA(5)
        snapshot3  <- IO.sleep(2.second) >> record
        snapshot4  <- IO.sleep(2.second) >> record
      } yield {
        assertEquals(
          snapshots1 ::: snapshots2 ::: List(snapshot3) ::: List(snapshot4),
          List(
            snap(totalMeasurements = 1, totalFailures = 0),
            snap(totalMeasurements = 2, totalFailures = 0),
            snap(totalMeasurements = 3, totalFailures = 0),
            snap(totalMeasurements = 4, totalFailures = 0),
            snap(totalMeasurements = 5, totalFailures = 0),
            snap(totalMeasurements = 6, totalFailures = 0),
            snap(totalMeasurements = 7, totalFailures = 0),
            snap(totalMeasurements = 8, totalFailures = 0),
            snap(totalMeasurements = 9, totalFailures = 0),
            // old bucket in window are cleared
            snap(totalMeasurements = 5, totalFailures = 0),
            snap(totalMeasurements = 4, totalFailures = 0),
            snap(totalMeasurements = 3, totalFailures = 0)
          )
        )
      }
    }
  }

  test("TimeBasedSlidingWindowMeasurements with all failures") {
    val minNumberOfCalls                                           = 1
    def snap(totalMeasurements: Int, totalFailures: Int): Snapshot =
      doSnap(minNumberOfCalls)(totalMeasurements, totalFailures)
    TestControl.executeEmbed {
      for {
        measurements <- TimeBasedSlidingWindowMeasurements[IO](
          numberOfBuckets = 5,
          bucketSize = 1.second,
          minNumberOfCalls = minNumberOfCalls
        )
        record = measurements.record(isFailure = true)
        snapshots1 <- record.replicateA(5)
        snapshots2 <- (IO.sleep(1.second) >> record).replicateA(5)
        snapshot3  <- IO.sleep(2.second) >> record
        snapshot4  <- IO.sleep(2.second) >> record
      } yield {
        assertEquals(
          snapshots1 ::: snapshots2 ::: List(snapshot3) ::: List(snapshot4),
          List(
            snap(totalMeasurements = 1, totalFailures = 1),
            snap(totalMeasurements = 2, totalFailures = 2),
            snap(totalMeasurements = 3, totalFailures = 3),
            snap(totalMeasurements = 4, totalFailures = 4),
            snap(totalMeasurements = 5, totalFailures = 5),
            snap(totalMeasurements = 6, totalFailures = 6),
            snap(totalMeasurements = 7, totalFailures = 7),
            snap(totalMeasurements = 8, totalFailures = 8),
            snap(totalMeasurements = 9, totalFailures = 9),
            // old bucket in window are cleared
            snap(totalMeasurements = 5, totalFailures = 5),
            snap(totalMeasurements = 4, totalFailures = 4),
            snap(totalMeasurements = 3, totalFailures = 3)
          )
        )
      }
    }
  }

  test("TimeBasedSlidingWindowMeasurements with steady failures") {
    val minNumberOfCalls                                           = 1
    def snap(totalMeasurements: Int, totalFailures: Int): Snapshot =
      doSnap(minNumberOfCalls)(totalMeasurements, totalFailures)
    TestControl.executeEmbed {
      for {
        measurements <- TimeBasedSlidingWindowMeasurements[IO](
          numberOfBuckets = 4,
          bucketSize = 1.second,
          minNumberOfCalls = minNumberOfCalls
        )
        recordWithDelay = (delay: FiniteDuration) =>
          IO.sleep(delay) >> measurements.record(isFailure = false).flatMap { snapshot1 =>
            IO.sleep(delay) >> measurements
              .record(isFailure = true)
              .map(snapshot2 => List(snapshot1, snapshot2))
          }
        snapshots1 <- recordWithDelay(0.seconds).replicateA(2)
        snapshots2 <- recordWithDelay(1.second).replicateA(2)
        snapshot3  <- IO.sleep(2.second) >> measurements.record(isFailure = false)
        snapshot4  <- IO.sleep(2.second) >> measurements.record(isFailure = true)
      } yield {
        assertEquals(
          snapshots1.flatten ::: snapshots2.flatten ::: List(snapshot3) ::: List(snapshot4),
          List(
            snap(totalMeasurements = 1, totalFailures = 0),
            snap(totalMeasurements = 2, totalFailures = 1),
            snap(totalMeasurements = 3, totalFailures = 1),
            snap(totalMeasurements = 4, totalFailures = 2), // [x, _, _, _] up to this point, in same bucket
            snap(totalMeasurements = 5, totalFailures = 2), // [o, x, _, _]
            snap(totalMeasurements = 6, totalFailures = 3), // [o, o, x, _]
            snap(totalMeasurements = 7, totalFailures = 3), // [o, o, o, x]
            snap(totalMeasurements = 4, totalFailures = 2), // [x, o, o, o] overwrite old bucket
            snap(totalMeasurements = 3, totalFailures = 1), // [o, _, x, o] overwrite old bucket, clear older ones
            snap(totalMeasurements = 2, totalFailures = 1)  // [x, _, o, _]
          )
        )
      }
    }
  }

  test("TimeBasedSlidingWindowMeasurements with sudden failures") {
    val minNumberOfCalls                                           = 1
    def snap(totalMeasurements: Int, totalFailures: Int): Snapshot =
      doSnap(minNumberOfCalls)(totalMeasurements, totalFailures)
    TestControl.executeEmbed {
      for {
        measurements <- TimeBasedSlidingWindowMeasurements[IO](
          numberOfBuckets = 4,
          bucketSize = 1.second,
          minNumberOfCalls = minNumberOfCalls
        )
        recordSuccess = measurements.record(isFailure = false)
        recordFailure = measurements.record(isFailure = true)
        snapshot0  <- (IO.sleep(1.second) >> recordSuccess).replicateA(8).map(_.last)
        snapshot1  <- IO.sleep(1.second) >> recordFailure
        snapshot2  <- recordSuccess
        snapshot3  <- recordFailure
        snapshot4  <- recordFailure
        snapshots5 <- (IO.sleep(2.second) >> recordSuccess).replicateA(2)
      } yield {
        assertEquals(
          List(snapshot0, snapshot1, snapshot2, snapshot3, snapshot4) ::: snapshots5,
          List(
            snap(totalMeasurements = 4, totalFailures = 0), // [x, o, o, o] base case
            snap(totalMeasurements = 4, totalFailures = 1), // [o, x, o, o] begin errors in same bucket
            snap(totalMeasurements = 5, totalFailures = 1),
            snap(totalMeasurements = 6, totalFailures = 2),
            snap(totalMeasurements = 7, totalFailures = 3),
            snap(totalMeasurements = 6, totalFailures = 3), // [o, o, _, x] overwrite old bucket, clear older one
            snap(
              totalMeasurements = 2,
              totalFailures = 0
            ) // [_, x, _, o] overwrite old bucket, clear older one with errors
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
        measurements <- TimeBasedSlidingWindowMeasurements[IO](
          numberOfBuckets = 3,
          bucketSize = 1.second,
          minNumberOfCalls = minNumberOfCalls
        )
        s1 <- measurements.record(isFailure = false)
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
  }

  test("peek returns the current snapshot without recording a measurement") {
    val minNumberOfCalls                                           = 2
    def snap(totalMeasurements: Int, totalFailures: Int): Snapshot =
      doSnap(minNumberOfCalls)(totalMeasurements, totalFailures)
    TestControl.executeEmbed {
      for {
        measurements <- TimeBasedSlidingWindowMeasurements[IO](
          numberOfBuckets = 3,
          bucketSize = 1.second,
          minNumberOfCalls = minNumberOfCalls
        )
        empty <- measurements.peek
        _ = assertEquals(empty, snap(totalMeasurements = 0, totalFailures = 0))
        recorded <- measurements.record(isFailure = true)
        _ = assertEquals(recorded, snap(totalMeasurements = 1, totalFailures = 1))
        peeked1 <- measurements.peek
        peeked2 <- measurements.peek
        _ = assertEquals(peeked1, snap(totalMeasurements = 1, totalFailures = 1))
        _ = assertEquals(peeked2, snap(totalMeasurements = 1, totalFailures = 1))
        next <- measurements.record(isFailure = false)
        _ = assertEquals(next, snap(totalMeasurements = 2, totalFailures = 1))
      } yield ()
    }
  }

  test("peek expires stale time buckets without adding a measurement") {
    val minNumberOfCalls                                           = 1
    def snap(totalMeasurements: Int, totalFailures: Int): Snapshot =
      doSnap(minNumberOfCalls)(totalMeasurements, totalFailures)
    TestControl.executeEmbed {
      for {
        measurements <- TimeBasedSlidingWindowMeasurements[IO](
          numberOfBuckets = 3,
          bucketSize = 1.second,
          minNumberOfCalls = minNumberOfCalls
        )
        _       <- measurements.record(isFailure = true)
        _       <- IO.sleep(4.seconds)
        expired <- measurements.peek
        _ = assertEquals(expired, snap(totalMeasurements = 0, totalFailures = 0))
        next <- measurements.record(isFailure = false)
        _ = assertEquals(next, snap(totalMeasurements = 1, totalFailures = 0))
      } yield ()
    }
  }

  test("recordings after reset produce correct snapshots") {
    val minNumberOfCalls                                           = 1
    def snap(totalMeasurements: Int, totalFailures: Int): Snapshot =
      doSnap(minNumberOfCalls)(totalMeasurements, totalFailures)
    TestControl.executeEmbed {
      for {
        measurements <- TimeBasedSlidingWindowMeasurements[IO](
          numberOfBuckets = 3,
          bucketSize = 1.second,
          minNumberOfCalls = minNumberOfCalls
        )
        _        <- measurements.record(isFailure = true).replicateA(5)
        snapshot <- measurements.record(isFailure = true)
        _ = assertEquals(snapshot, snap(totalMeasurements = 6, totalFailures = 6))

        _ <- measurements.reset

        s1 <- measurements.record(isFailure = false)
        _ = assertEquals(s1, snap(totalMeasurements = 1, totalFailures = 0))
        s2 <- measurements.record(isFailure = true)
        _ = assertEquals(s2, snap(totalMeasurements = 2, totalFailures = 1))
        s3 <- IO.sleep(1.second) >> measurements.record(isFailure = false)
        _ = assertEquals(s3, snap(totalMeasurements = 3, totalFailures = 1))
      } yield ()
    }
  }
}

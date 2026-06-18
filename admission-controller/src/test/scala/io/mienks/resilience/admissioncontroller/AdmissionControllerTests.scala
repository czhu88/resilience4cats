package io.mienks.resilience.admissioncontroller

import cats.effect.IO
import cats.effect.std.Random
import cats.syntax.all._
import io.mienks.resilience.Measurements.Snapshot
import io.mienks.resilience.admissioncontroller.AdmissionController.MeasurementStrategy
import munit.CatsEffectSuite

class AdmissionControllerTests extends CatsEffectSuite {

  test("rejectionProbability is zero until the measurement window is initialized") {
    val snapshot = Snapshot(totalMeasurements = 1, totalFailures = 1, isInitialized = false)

    IO(assertEquals(AdmissionController.rejectionProbability(snapshot = snapshot, k = 2.0), 0.0))
  }

  test("rejectionProbability follows proportional shedding math") {
    val snapshot = Snapshot(totalMeasurements = 10, totalFailures = 5, isInitialized = true)

    IO(assertEquals(AdmissionController.rejectionProbability(snapshot = snapshot, k = 1.5), 2.5 / 11.0))
  }

  test("rejectionProbability is zero when requests are within the accepted request budget") {
    val snapshot = Snapshot(totalMeasurements = 10, totalFailures = 1, isInitialized = true)

    IO(assertEquals(AdmissionController.rejectionProbability(snapshot = snapshot, k = 2.0), 0.0))
  }

  test("allow permits traffic before the window is initialized") {
    val config = AdmissionController.Config(
      measurementStrategy = MeasurementStrategy.CountBasedSlidingWindow(windowSize = 10, minNumberOfCalls = 2),
      k = 2.0
    )

    for {
      controller <- AdmissionController[IO](config = config)
      allowed1   <- controller.allow
      _          <- controller.record(isFailure = true)
      allowed2   <- controller.allow
    } yield {
      assert(allowed1)
      assert(allowed2)
    }
  }

  test("allow adjusts as failures rise and recover") {
    val config = AdmissionController.Config(
      measurementStrategy = MeasurementStrategy.CountBasedSlidingWindow(windowSize = 4, minNumberOfCalls = 1),
      k = 2.0
    )

    for {
      random               <- Random.javaUtilRandom[IO](new java.util.Random(0L))
      controller           <- AdmissionController[IO](config = config, random = random)
      _                    <- controller.record(isFailure = false).replicateA_(4)
      initiallyAllowed     <- controller.allow
      _                    <- controller.record(isFailure = true).replicateA_(4)
      allowedUnderFailures <- controller.allow
      _                    <- controller.record(isFailure = false).replicateA_(4)
      allowedAfterRecovery <- controller.allow
    } yield {
      assert(initiallyAllowed)
      assert(!allowedUnderFailures)
      assert(allowedAfterRecovery)
    }
  }

  test("recording test double tracks snapshots") {
    for {
      controller <- AdmissionController.recording[IO](canAllow = true.pure[IO])
      allowed    <- controller.allow
      _          <- controller.record(isFailure = false)
      _          <- controller.record(isFailure = true)
      snapshot   <- controller.snapshot
    } yield {
      assert(allowed)
      assertEquals(snapshot, Snapshot(totalMeasurements = 2, totalFailures = 1, isInitialized = true))
    }
  }
}

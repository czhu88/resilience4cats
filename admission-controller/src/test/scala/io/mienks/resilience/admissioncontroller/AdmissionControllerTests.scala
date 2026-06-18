package io.mienks.resilience.admissioncontroller

import cats.effect.IO
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

  test("record delegates to the configured measurement window") {
    val config = AdmissionController.Config(
      measurementStrategy = MeasurementStrategy.CountBasedSlidingWindow(windowSize = 2, minNumberOfCalls = 1),
      k = 2.0
    )

    for {
      controller <- AdmissionController[IO](config = config)
      s1         <- controller.record(isFailure = true)
      s2         <- controller.record(isFailure = false)
      s3         <- controller.record(isFailure = false)
    } yield {
      assertEquals(s1, Snapshot(totalMeasurements = 1, totalFailures = 1, isInitialized = true))
      assertEquals(s2, Snapshot(totalMeasurements = 2, totalFailures = 1, isInitialized = true))
      assertEquals(s3, Snapshot(totalMeasurements = 2, totalFailures = 0, isInitialized = true))
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

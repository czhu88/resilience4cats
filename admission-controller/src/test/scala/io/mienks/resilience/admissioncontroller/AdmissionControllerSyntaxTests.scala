package io.mienks.resilience.admissioncontroller

import cats.effect.{IO, Ref}
import cats.syntax.all._
import io.mienks.resilience.Measurements.Snapshot
import io.mienks.resilience.admissioncontroller.AdmissionController.syntax._
import munit.CatsEffectSuite

class AdmissionControllerSyntaxTests extends CatsEffectSuite {

  test("protect records an admitted success") {
    for {
      controller <- AdmissionController.recording[IO](canAllow = true.pure[IO])
      out        <- controller.protect(fa = IO("ok"), isFailure = (_: String) => false, orElse = "fallback")
      snapshot   <- controller.snapshot
    } yield {
      assertEquals(out, "ok")
      assertEquals(snapshot, Snapshot(totalMeasurements = 1, totalFailures = 0, isInitialized = true))
    }
  }

  test("protect records an admitted failure") {
    for {
      controller <- AdmissionController.recording[IO](canAllow = true.pure[IO])
      out        <- controller.protect(fa = IO("boom"), isFailure = (_: String) => true, orElse = "fallback")
      snapshot   <- controller.snapshot
    } yield {
      assertEquals(out, "boom")
      assertEquals(snapshot, Snapshot(totalMeasurements = 1, totalFailures = 1, isInitialized = true))
    }
  }

  test("protect records local rejection as a failure without running fa") {
    for {
      controller <- AdmissionController.recording[IO](canAllow = false.pure[IO])
      ran        <- Ref[IO].of(false)
      out      <- controller.protect(fa = ran.set(true).as("ok"), isFailure = (_: String) => false, orElse = "fallback")
      didRun   <- ran.get
      snapshot <- controller.snapshot
    } yield {
      assertEquals(out, "fallback")
      assert(!didRun)
      assertEquals(snapshot, Snapshot(totalMeasurements = 1, totalFailures = 1, isInitialized = true))
    }
  }

  test("protectF records effectfully classified outcomes") {
    for {
      controller <- AdmissionController.recording[IO](canAllow = true.pure[IO])
      out        <- controller.protectF(
        fa = IO("ok"),
        isFailure = (_: String) => false.pure[IO],
        orElse = "fallback".pure[IO]
      )
      snapshot <- controller.snapshot
    } yield {
      assertEquals(out, "ok")
      assertEquals(snapshot, Snapshot(totalMeasurements = 1, totalFailures = 0, isInitialized = true))
    }
  }
}

package io.mienks.resilience.admissioncontroller

import cats.Applicative
import cats.syntax.all._
import io.mienks.resilience.Measurements.Snapshot

/** Always-allowing [[AdmissionController]] for testing or as a safe default when admission control is disabled. */
class NoopAdmissionController[F[_]: Applicative] extends AdmissionController[F] {

  override def allow: F[Boolean] = true.pure[F]

  override def record(isFailure: Boolean): F[Snapshot] =
    Snapshot(totalMeasurements = 0, totalFailures = 0, isInitialized = false).pure[F]

  override def rejectionProbability: F[Double] = 0.0.pure[F]
}

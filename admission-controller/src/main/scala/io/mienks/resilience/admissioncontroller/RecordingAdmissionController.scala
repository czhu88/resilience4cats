package io.mienks.resilience.admissioncontroller

import cats.effect.{Ref, Sync}
import cats.syntax.all._
import io.mienks.resilience.Measurements.Snapshot

/** Test double for [[AdmissionController]] that delegates `allow` to the supplied effect and records outcomes in
  * memory.
  */
class RecordingAdmissionController[F[_]: Sync] private[admissioncontroller] (
    canAllow: F[Boolean],
    private val state: Ref[F, RecordingAdmissionController.State]
) extends NoopAdmissionController[F] {

  override def allow: F[Boolean] = canAllow

  override def record(isFailure: Boolean): F[Snapshot] =
    state.modify(_.record(isFailure = isFailure))

  override def rejectionProbability: F[Double] =
    state.get.map(s => AdmissionController.rejectionProbability(snapshot = s.snapshot, k = 2.0))
  def snapshot: F[Snapshot] =
    state.get.map(_.snapshot)
}

object RecordingAdmissionController {

  private[admissioncontroller] final case class State(totalMeasurements: Int, totalFailures: Int) {

    def record(isFailure: Boolean): (State, Snapshot) = {
      val updated = copy(
        totalMeasurements = totalMeasurements + 1,
        totalFailures = totalFailures + (if (isFailure) 1 else 0)
      )
      (updated, updated.snapshot)
    }

    def snapshot: Snapshot =
      Snapshot(
        totalMeasurements = totalMeasurements,
        totalFailures = totalFailures,
        isInitialized = totalMeasurements > 0
      )
  }

  private[admissioncontroller] object State {
    val empty: State = State(totalMeasurements = 0, totalFailures = 0)
  }
}

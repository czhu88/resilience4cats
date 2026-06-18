package io.mienks.resilience.charts.admission

import cats.data.NonEmptyList
import io.mienks.resilience.Rate
import io.mienks.resilience.admissioncontroller.AdmissionController.{Config, MeasurementStrategy}
import io.mienks.resilience.charts.Backend

import scala.concurrent.duration._

/** A closed-loop run against an [[io.mienks.resilience.admissioncontroller.AdmissionController]]: a constant, heavy
  * workload offers load through the controller to a [[Backend]] whose capacity follows `phases`. Any call admitted
  * above the backend's hard ceiling is "throttled" (a failure), which is the downstream-overload signal the controller
  * sheds against.
  *
  * @param name
  *   slug used for output file names
  * @param description
  *   one-line caption rendered on the chart
  * @param config
  *   controller configuration for the run
  * @param phases
  *   ordered backend-capacity phases (see [[Backend.Phase]]); the head phase's duration includes the warmup
  */
final case class AdmissionScenario(
    name: String,
    description: String,
    config: Config,
    phases: NonEmptyList[Backend.Phase]
) {
  def totalDuration: FiniteDuration = phases.toList.map(_.duration).reduce(_ + _)
}

object AdmissionScenario {

  private def rps(requests: Int): Rate = Rate(requests = requests, period = 1.second)

  // Millisecond time scale so a full run takes a few seconds. The offered load is held well above the degraded
  // capacities so the controller has excess to shed; rps magnitudes stay in the hundreds so each measurement window
  // still collects enough admitted requests for the failure ratio to be meaningful.
  private val SlotDuration: FiniteDuration = 25.millis
  private val MeasurementBuckets: Int      = 4
  private val MinNumberOfCalls: Int        = 20

  // Settle time spent in the first phase before the scripted capacity changes begin; folded into each head duration.
  private val Warmup: FiniteDuration = 500.millis

  private val BaseConfig: Config =
    Config(
      measurementStrategy = MeasurementStrategy.TimeBasedSlidingWindow(
        numberOfBuckets = MeasurementBuckets,
        bucketSize = SlotDuration,
        minNumberOfCalls = MinNumberOfCalls
      ),
      k = 2.0
    )

  // Capacity comfortably above the offered load (~800 rps, see AdmissionSimulation) keeps the backend healthy: the
  // controller admits everything and the rejection probability sits at zero.
  private val Healthy: Rate = rps(1200)

  private val steadyOverload: AdmissionScenario =
    AdmissionScenario(
      name = "steady-overload",
      description =
        "Constant capacity below offered load: the controller sheds the excess, holding admitted near k*capacity " +
          "and goodput near capacity (a smooth plateau, no sawtooth).",
      config = BaseConfig,
      phases =
        NonEmptyList.one(Backend.Phase(hardCeiling = rps(150), softCeilings = Nil, duration = Warmup + 7.seconds))
    )

  private val capacityDrop: AdmissionScenario =
    AdmissionScenario(
      name = "capacity-drop",
      description = "A sudden capacity drop pushes the rejection probability from zero up to a steady shedding level.",
      config = BaseConfig,
      phases = NonEmptyList.of(
        Backend.Phase(hardCeiling = Healthy, softCeilings = Nil, duration = Warmup + 1.second),
        Backend.Phase(hardCeiling = rps(130), softCeilings = Nil, duration = 5.seconds)
      )
    )

  private val dropThenRecover: AdmissionScenario =
    AdmissionScenario(
      name = "drop-then-recover",
      description =
        "After a capacity collapse and shedding, restored capacity drives the rejection probability back to zero " +
          "(k>1 guarantees the gate fully reopens).",
      config = BaseConfig,
      phases = NonEmptyList.of(
        Backend.Phase(hardCeiling = Healthy, softCeilings = Nil, duration = Warmup + 1.second),
        Backend.Phase(hardCeiling = rps(130), softCeilings = Nil, duration = 4.seconds),
        Backend.Phase(hardCeiling = Healthy, softCeilings = Nil, duration = 5.seconds)
      )
    )

  private val slowDegradation: AdmissionScenario =
    AdmissionScenario(
      name = "slow-degradation",
      description = "Capacity steps down gradually; the rejection probability steps up to match each lower ceiling.",
      config = BaseConfig,
      phases = NonEmptyList.of(
        Backend.Phase(hardCeiling = Healthy, softCeilings = Nil, duration = Warmup + 1.second),
        Backend.Phase(hardCeiling = rps(280), softCeilings = Nil, duration = 3.seconds),
        Backend.Phase(hardCeiling = rps(180), softCeilings = Nil, duration = 3.seconds),
        Backend.Phase(hardCeiling = rps(110), softCeilings = Nil, duration = 3.seconds)
      )
    )

  val all: List[AdmissionScenario] = List(
    steadyOverload,
    capacityDrop,
    dropThenRecover,
    slowDegradation
  )
}

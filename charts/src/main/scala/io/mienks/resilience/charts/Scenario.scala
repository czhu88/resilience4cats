package io.mienks.resilience.charts

import cats.data.NonEmptyList
import io.mienks.resilience.Rate
import io.mienks.resilience.adaptiveratelimiter.AdaptiveRateLimiter.{Config, HysteresisBand}

import scala.concurrent.duration._

/** One leg of a scenario: hold the simulated backend at `failureRatio` for `duration`. */
final case class Segment(failureRatio: Double, duration: FiniteDuration)

/** A scripted run against an [[io.mienks.resilience.adaptiveratelimiter.AdaptiveRateLimiter]].
  *
  * @param name
  *   slug used for output file names
  * @param description
  *   one-line caption rendered on the chart
  * @param config
  *   limiter configuration for the run
  * @param warmup
  *   settle time at the first segment's ratio before sampling-relevant behavior is interesting (still sampled)
  * @param segments
  *   ordered backend-health phases driven into the limiter
  */
final case class Scenario(
    name: String,
    description: String,
    config: Config,
    warmup: FiniteDuration,
    segments: List[Segment]
)

object Scenario {

  // Scaled-down so each scenario runs in a few real seconds while keeping enough sample resolution.
  private val SlotDuration: FiniteDuration       = 25.millis
  private val MeasurementBuckets: Int            = 4
  private val MeasurementWindow: FiniteDuration  = SlotDuration * MeasurementBuckets // 100ms
  private val RateIncreasePeriod: FiniteDuration = 40.millis

  private val Bands: NonEmptyList[HysteresisBand] = NonEmptyList.of(
    HysteresisBand(exit = 0.1, start = 0.3), // level 0: minor degradation
    HysteresisBand(exit = 0.4, start = 0.7)  // level 1: severe degradation
  )

  private val BaseConfig: Config =
    Config(
      capacity = 10,
      initialRate = Rate(requests = 2, period = 1.second),
      minRate = Rate(requests = 1, period = 1.second),
      maxRate = Rate(requests = 10, period = 1.second),
      rateIncreaseBy = Rate(requests = 2, period = 1.second),
      rateIncreasePeriod = RateIncreasePeriod,
      rateDecreaseBy = 0.5,
      numberOfSlotsForMeasurements = MeasurementBuckets,
      slotDuration = SlotDuration,
      measurementPeriod = SlotDuration,
      minNumberOfMeasurements = MeasurementBuckets,
      failureLevels = Bands
    )

  /** All scenarios mirror the documented behaviors in `AdaptiveRateLimiterTests`. */
  val library: List[Scenario] = List(
    healthyClimb,
    quickDegradation,
    degradeThenRecover,
    slowRecovery,
    flapping,
    slowDegradation
  )

  private def healthyClimb: Scenario =
    Scenario(
      name = "healthy-climb",
      description = "Healthy backend: the AIMD estimate climbs additively to maxRate and stays there.",
      config = BaseConfig,
      warmup = MeasurementWindow,
      segments = List(Segment(failureRatio = 0.0, duration = MeasurementWindow * 16))
    )

  private def quickDegradation: Scenario =
    Scenario(
      name = "quick-degradation",
      description = "A failure spike promotes through both bands and multiplicatively cuts the rate.",
      config = BaseConfig,
      warmup = MeasurementWindow,
      segments = List(
        Segment(failureRatio = 0.0, duration = MeasurementWindow * 12),
        Segment(failureRatio = 0.8, duration = MeasurementWindow * 10)
      )
    )

  private def degradeThenRecover: Scenario =
    Scenario(
      name = "degrade-then-recover",
      description = "After a spike and rate cut, a recovered backend returns to Healthy and climbs back to max.",
      config = BaseConfig,
      warmup = MeasurementWindow,
      segments = List(
        Segment(failureRatio = 0.0, duration = MeasurementWindow * 12),
        Segment(failureRatio = 0.8, duration = MeasurementWindow * 8),
        Segment(failureRatio = 0.0, duration = MeasurementWindow * 16)
      )
    )

  private def slowRecovery: Scenario =
    Scenario(
      name = "slow-recovery",
      description = "Errors clearing gradually hold the failing tier (hysteresis) until fully below the exit band.",
      config = BaseConfig.copy(initialRate = BaseConfig.maxRate),
      warmup = MeasurementWindow,
      segments = List(
        Segment(failureRatio = 0.0, duration = MeasurementWindow * 8),
        Segment(failureRatio = 0.8, duration = MeasurementWindow * 6), // spike to the severe tier
        Segment(failureRatio = 0.5, duration = MeasurementWindow * 6), // linger inside band 1
        Segment(failureRatio = 0.2, duration = MeasurementWindow * 6), // demote to band 0, still above exit
        Segment(failureRatio = 0.0, duration = MeasurementWindow * 12) // full clear -> Recovered
      )
    )

  private def flapping: Scenario =
    Scenario(
      name = "flapping",
      description = "Repeated degrade/recover cycles ratchet the rate down toward the min floor.",
      // Slow the additive recovery so the rate barely climbs between flaps; compounding cuts then reach the floor.
      config = BaseConfig.copy(initialRate = BaseConfig.maxRate, rateIncreasePeriod = 1.second),
      warmup = MeasurementWindow,
      segments = List
        .fill(4)(
          List(
            Segment(failureRatio = 0.8, duration = MeasurementWindow * 4),
            Segment(failureRatio = 0.0, duration = MeasurementWindow * 4)
          )
        )
        .flatten
    )

  private def slowDegradation: Scenario =
    Scenario(
      name = "slow-degradation",
      description = "Rising errors trip the bands one tier at a time, compounding the rate cut.",
      // Slow the additive recovery so the first tier's cut is still visible when the second tier trips.
      config = BaseConfig.copy(initialRate = BaseConfig.maxRate, rateIncreasePeriod = 1.second),
      warmup = MeasurementWindow,
      segments = List(
        Segment(failureRatio = 0.0, duration = MeasurementWindow * 6),
        Segment(failureRatio = 0.4, duration = MeasurementWindow * 10), // trips only the first tier
        Segment(failureRatio = 0.8, duration = MeasurementWindow * 10)  // trips the second tier
      )
    )
}

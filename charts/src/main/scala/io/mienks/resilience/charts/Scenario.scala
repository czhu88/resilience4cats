package io.mienks.resilience.charts

import cats.data.NonEmptyList
import io.mienks.resilience.Rate
import io.mienks.resilience.adaptiveratelimiter.AdaptiveRateLimiter.{Config, HysteresisBand}

import scala.concurrent.duration._

/** One leg of a scenario: hold the simulated backend's base capacity at `capacity` for `duration`. A lower capacity is
  * a degraded backend; raising it again is recovery.
  */
final case class Segment(capacity: Rate, duration: FiniteDuration)

/** A closed-loop run against an [[io.mienks.resilience.adaptiveratelimiter.AdaptiveRateLimiter]]: a workload offers
  * load through the limiter to a [[Backend]] whose capacity follows `segments`, and failures emerge from the limiter
  * over-driving that capacity.
  *
  * @param name
  *   slug used for output file names
  * @param description
  *   one-line caption rendered on the chart
  * @param config
  *   limiter configuration for the run
  * @param backendTiers
  *   capacity tiers of the simulated backend (see [[Backend]])
  * @param warmup
  *   settle time at the first segment's capacity before the scripted capacity changes begin
  * @param segments
  *   ordered backend-capacity phases
  */
final case class Scenario(
    name: String,
    description: String,
    config: Config,
    backendTiers: NonEmptyList[Backend.Tier],
    warmup: FiniteDuration,
    segments: List[Segment]
)

object Scenario {

  private def rps(requests: Int): Rate = Rate(requests = requests, period = 1.second)

  // Millisecond time scale so a full run takes a few seconds. The rps magnitudes stay in the hundreds because the
  // closed loop derives the failure ratio from requests the limiter actually admits, so each 100ms measurement window
  // needs enough admitted requests (>= minNumberOfMeasurements) for the ratio to be meaningful.
  private val SlotDuration: FiniteDuration = 25.millis
  private val MeasurementBuckets: Int      = 4

  private val Bands: NonEmptyList[HysteresisBand] = NonEmptyList.of(
    HysteresisBand(exit = 0.1, start = 0.3), // level 0: minor degradation
    HysteresisBand(exit = 0.4, start = 0.7)  // level 1: severe degradation
  )

  private val BaseConfig: Config =
    Config(
      capacity = 40,
      initialRate = rps(50),
      // Floor high enough that the 100ms window still collects >= minNumberOfMeasurements admitted requests.
      minRate = rps(50),
      maxRate = rps(400),
      // Additive increase of ~125 rps/s: a brisk climb that still shows clear sawtooth ramps.
      rateIncreaseBy = rps(10),
      rateIncreasePeriod = 80.millis,
      rateDecreaseBy = 0.5,
      numberOfSlotsForMeasurements = MeasurementBuckets,
      slotDuration = SlotDuration,
      measurementPeriod = SlotDuration,
      minNumberOfMeasurements = MeasurementBuckets,
      failureLevels = Bands
    )

  // A capacity comfortably above maxRate keeps the backend healthy (no overload), so the rate plateaus at max.
  private val Healthy: Rate = rps(500)

  val library: List[Scenario] = List(
    congestionSawtooth,
    quickDegradation,
    degradeThenRecover,
    slowRecovery,
    flapping,
    slowDegradation,
    gradedDegradation
  )

  private def congestionSawtooth: Scenario =
    Scenario(
      name = "congestion-sawtooth",
      description = "Constant backend capacity below maxRate: AIMD oscillates around it in the classic TCP sawtooth.",
      config = BaseConfig,
      backendTiers = Backend.Single,
      warmup = 500.millis,
      segments = List(Segment(capacity = rps(200), duration = 7.seconds))
    )

  private def quickDegradation: Scenario =
    Scenario(
      name = "quick-degradation",
      description = "A sudden capacity drop trips a band and cuts the rate into a lower sawtooth around the capacity.",
      config = BaseConfig,
      backendTiers = Backend.Single,
      warmup = 500.millis,
      segments = List(
        Segment(capacity = Healthy, duration = 1.second),
        Segment(capacity = rps(130), duration = 5.seconds)
      )
    )

  private def degradeThenRecover: Scenario =
    Scenario(
      name = "degrade-then-recover",
      description = "After a capacity collapse and rate cut, restored capacity returns the backend to healthy and max.",
      config = BaseConfig,
      backendTiers = Backend.Single,
      warmup = 500.millis,
      segments = List(
        Segment(capacity = Healthy, duration = 1.second),
        Segment(capacity = rps(130), duration = 4.seconds),
        Segment(capacity = Healthy, duration = 5.seconds)
      )
    )

  private def slowRecovery: Scenario =
    Scenario(
      name = "slow-recovery",
      description = "Capacity recovers in steps; the limiter climbs back to a higher safe rate at each step.",
      config = BaseConfig,
      backendTiers = Backend.Single,
      warmup = 500.millis,
      segments = List(
        Segment(capacity = Healthy, duration = 1.second),
        Segment(capacity = rps(130), duration = 2500.millis), // severe
        Segment(capacity = rps(200), duration = 2500.millis), // partial
        Segment(capacity = rps(320), duration = 2500.millis), // more headroom
        Segment(capacity = Healthy, duration = 3.seconds)     // full clear
      )
    )

  private def flapping: Scenario =
    Scenario(
      name = "flapping",
      description = "Capacity flaps between healthy and degraded: compounding cuts ratchet the rate toward the floor.",
      config = BaseConfig,
      backendTiers = Backend.Single,
      warmup = 500.millis,
      // Degraded windows longer than recovery windows so the cuts dominate and the rate ratchets down.
      segments = List
        .fill(5)(
          List(
            Segment(capacity = rps(60), duration = 1500.millis),
            Segment(capacity = Healthy, duration = 800.millis)
          )
        )
        .flatten
    )

  private def slowDegradation: Scenario =
    Scenario(
      name = "slow-degradation",
      description = "Capacity steps down gradually; the limiter re-discovers a lower safe rate at each step.",
      config = BaseConfig,
      backendTiers = Backend.Single,
      warmup = 500.millis,
      segments = List(
        Segment(capacity = Healthy, duration = 1.second),
        Segment(capacity = rps(150), duration = 4.seconds), // mild: a sawtooth around the new capacity
        Segment(capacity = rps(70), duration = 4.seconds)   // lower: a tighter sawtooth around the new capacity
      )
    )

  private def gradedDegradation: Scenario =
    Scenario(
      name = "graded-degradation",
      description = "A two-tier backend (soft + hard ceiling) produces two distinct failure levels across the bands.",
      config = BaseConfig,
      // Soft ceiling at half the base capacity fails 50% of its overflow; the hard ceiling at the base always fails.
      backendTiers = NonEmptyList.of(
        Backend.Tier(relativeCapacity = 0.5, failProbability = 0.5),
        Backend.Tier(relativeCapacity = 1.0, failProbability = 1.0)
      ),
      warmup = 500.millis,
      segments = List(
        Segment(capacity = Healthy, duration = 1.second),
        Segment(capacity = rps(80), duration = 5.seconds), // severe: the soft then hard ceiling trip both bands
        Segment(capacity = Healthy, duration = 4.seconds)  // restored: the backend recovers and the rate climbs back
      )
    )
}

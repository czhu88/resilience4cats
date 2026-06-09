package io.mienks.resilience.charts

import cats.effect.{IO, Ref}
import cats.syntax.all._
import io.mienks.resilience.Rate
import io.mienks.resilience.adaptiveratelimiter.AdaptiveRateLimiter
import io.mienks.resilience.adaptiveratelimiter.AdaptiveRateLimiter.FailureGradient
import io.mienks.resilience.adaptiveratelimiter.AdaptiveRateLimiter.syntax._

import java.util.concurrent.TimeUnit
import scala.concurrent.duration._

/** One sampled point of a closed-loop run.
  *
  * @param aimdRps
  *   the AIMD's current rate estimate (the controlled variable)
  * @param admittedRps
  *   the measured throughput the limiter actually admitted over the last sample interval
  * @param backendCapacityRps
  *   the backend's current sustainable capacity (the bottleneck the AIMD is trying to discover)
  * @param observedFailureRatio
  *   the limiter's sampled failure ratio in `[0, 1]`
  */
final case class Sample(
    elapsedMillis: Long,
    aimdRps: Double,
    admittedRps: Double,
    backendCapacityRps: Double,
    observedFailureRatio: Double
)

/** Everything a chart needs for one scenario: the dense sampled timeseries plus the discrete control-loop events. */
final case class RunResult(
    scenario: Scenario,
    samples: Vector[Sample],
    rateEvents: Vector[(Long, Double)],
    gradientEvents: Vector[(Long, FailureGradient)]
)

object SimulationRunner {

  // Several offer fibers, each attempting roughly every OfferInterval, so the offered load comfortably exceeds maxRate
  // and the admitted rate tracks the limiter's current refill rate.
  private val OfferWorkers: Int             = 4
  private val OfferInterval: FiniteDuration = 1.millis

  // Sampled finer than the slot duration so band crossings and rate cuts are visible in the timeseries.
  private val SamplePeriod: FiniteDuration = 50.millis

  // Fixed seed keeps the graded backend's probabilistic failures reproducible across runs.
  private val Seed: Long = 42L

  def run(scenario: Scenario): IO[RunResult] =
    for {
      samplesRef        <- Ref[IO].of(Vector.empty[Sample])
      rateEventsRef     <- Ref[IO].of(Vector.empty[(Long, Double)])
      gradientEventsRef <- Ref[IO].of(Vector.empty[(Long, FailureGradient)])
      admittedRef       <- Ref[IO].of(0L)
      initialCapacity = scenario.segments.headOption.fold(scenario.config.maxRate)(_.capacity)
      capacityRef   <- Ref[IO].of(initialCapacity)
      backend       <- Backend.create(base = initialCapacity, tiers = scenario.backendTiers, seed = Seed)
      startNanos    <- IO.monotonic
      lastSampleRef <- Ref[IO].of((startNanos.toNanos, 0L))
      elapsedMillis = IO.monotonic.map(now => (now - startNanos).toMillis)
      _ <- AdaptiveRateLimiter
        .start[IO](
          config = scenario.config,
          onFailureCategoryChange =
            (event: FailureGradient) => elapsedMillis.flatMap(ms => gradientEventsRef.update(_ :+ (ms, event))),
          onRateChange = (rate: Rate) => elapsedMillis.flatMap(ms => rateEventsRef.update(_ :+ (ms, toRps(rate))))
        )
        .use { limiter =>
          val offer =
            limiter.protect(
              fa = admittedRef.update(_ + 1L) >> backend.call,
              isError = (ok: Boolean) => !ok,
              orElse = true
            ) >> IO.sleep(OfferInterval)

          val takeSample =
            for {
              now      <- IO.monotonic
              admitted <- admittedRef.get
              prev     <- lastSampleRef.getAndSet((now.toNanos, admitted))
              (prevNanos, prevAdmitted) = prev
              dtSeconds                 = (now.toNanos - prevNanos).toDouble / 1e9
              admittedRps               = if (dtSeconds > 0.0) (admitted - prevAdmitted).toDouble / dtSeconds else 0.0
              observed <- limiter.failureRatio
              rate     <- limiter.rate
              capacity <- capacityRef.get
              _        <- samplesRef.update(
                _ :+ Sample(
                  elapsedMillis = (now - startNanos).toMillis,
                  aimdRps = toRps(rate),
                  admittedRps = admittedRps,
                  backendCapacityRps = toRps(capacity),
                  observedFailureRatio = observed
                )
              )
            } yield ()

          val drive =
            IO.sleep(scenario.warmup) >>
              scenario.segments.traverse_ { segment =>
                capacityRef.set(segment.capacity) >>
                  backend.setCapacity(segment.capacity) >>
                  IO.sleep(segment.duration)
              }

          offer.foreverM.background
            .replicateA_(OfferWorkers)
            .surround((takeSample >> IO.sleep(SamplePeriod)).foreverM.background.surround(drive))
        }
      samples        <- samplesRef.get
      rateEvents     <- rateEventsRef.get
      gradientEvents <- gradientEventsRef.get
    } yield RunResult(
      scenario = scenario,
      samples = samples,
      rateEvents = rateEvents,
      gradientEvents = gradientEvents
    )

  private def toRps(rate: Rate): Double =
    rate.requests.toDouble / rate.period.toUnit(TimeUnit.SECONDS)
}

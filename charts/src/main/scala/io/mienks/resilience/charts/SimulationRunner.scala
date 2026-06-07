package io.mienks.resilience.charts

import cats.effect.{IO, Ref}
import cats.syntax.all._
import io.mienks.resilience.Rate
import io.mienks.resilience.adaptiveratelimiter.AdaptiveRateLimiter
import io.mienks.resilience.adaptiveratelimiter.AdaptiveRateLimiter.FailureGradient

import java.util.concurrent.TimeUnit
import scala.concurrent.duration._

/** One sampled point of the run. `rps` is the AIMD's current estimate expressed as requests/second. */
final case class Sample(
    elapsedMillis: Long,
    targetFailureRatio: Double,
    observedFailureRatio: Double,
    rps: Double
)

/** Everything a chart needs for one scenario: the dense sampled timeseries plus the discrete control-loop events. */
final case class RunResult(
    scenario: Scenario,
    samples: Vector[Sample],
    rateEvents: Vector[(Long, Double)],
    gradientEvents: Vector[(Long, FailureGradient)]
)

object SimulationRunner {

  // Outcomes recorded per measurement period; large enough that `round(hits * ratio)` tracks the target ratio closely.
  private val BackendHitsPerRound: Int = 50

  // Sampled finer than the slot duration so band crossings and rate cuts are visible in the timeseries.
  private val SamplePeriod: FiniteDuration = 15.millis

  def run(scenario: Scenario): IO[RunResult] =
    for {
      ratioRef          <- Ref[IO].of(scenario.segments.headOption.fold(0.0)(_.failureRatio))
      samplesRef        <- Ref[IO].of(Vector.empty[Sample])
      rateEventsRef     <- Ref[IO].of(Vector.empty[(Long, Double)])
      gradientEventsRef <- Ref[IO].of(Vector.empty[(Long, FailureGradient)])
      startNanos        <- IO.monotonic
      elapsedMillis = IO.monotonic.map(now => (now - startNanos).toMillis)
      _ <- AdaptiveRateLimiter
        .start[IO](
          config = scenario.config,
          onFailureCategoryChange =
            (event: FailureGradient) => elapsedMillis.flatMap(ms => gradientEventsRef.update(_ :+ (ms, event))),
          onRateChange = (rate: Rate) => elapsedMillis.flatMap(ms => rateEventsRef.update(_ :+ (ms, toRps(rate))))
        )
        .use { limiter =>
          val hitBackend =
            ratioRef.get.flatMap { ratio =>
              val failures = math.round(BackendHitsPerRound * ratio).toInt
              limiter.recordFailure.replicateA_(failures) >>
                limiter.recordSuccess.replicateA_(BackendHitsPerRound - failures)
            } >> IO.sleep(scenario.config.measurementPeriod)

          val takeSample =
            for {
              ms       <- elapsedMillis
              target   <- ratioRef.get
              observed <- limiter.failureRatio
              rate     <- limiter.rate
              _        <- samplesRef.update(
                _ :+ Sample(
                  elapsedMillis = ms,
                  targetFailureRatio = target,
                  observedFailureRatio = observed,
                  rps = toRps(rate)
                )
              )
            } yield ()

          val drive =
            IO.sleep(scenario.warmup) >>
              scenario.segments.traverse_(segment => ratioRef.set(segment.failureRatio) >> IO.sleep(segment.duration))

          (hitBackend.foreverM.background, (takeSample >> IO.sleep(SamplePeriod)).foreverM.background).tupled
            .surround(drive)
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

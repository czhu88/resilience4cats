package io.mienks.resilience.adaptiveratelimiter

import cats.data.NonEmptyList
import cats.effect.{Async, Ref, Resource, Spawn, Sync, Temporal}
import cats.kernel.Eq
import cats.syntax.all._
import cats.{Applicative, ApplicativeThrow, Monad}
import fs2.{Chunk, Pipe}
import io.mienks.resilience.ratelimiter.{DynamicRateLimiter, RateLimiter}
import io.mienks.resilience.{Measurements, Rate, SampledMeasurements}

import scala.concurrent.duration._

/** Estimates and self-tunes the estimated rate at which a downstream resource (the "protected sink") can be invoked.
  * Internally wraps a `DynamicRateLimiter` whose refill rate is driven by an AIMD (Additive Increase / Multiplicative
  * Decrease) control loop reacting to observed failure rates.
  *
  * Outcomes are recorded with [[recordSuccess]] / [[recordFailure]] on a hot path (cheap, lock-free), then sampled on a
  * background fiber. The categorizer applies hysteresis bands to avoid flapping. The AIMD loop additively grows the
  * estimated rate on a fixed time tick and multiplicatively shrinks it when the failure-rate categorizer reports a
  * worsening signal.
  */
trait AdaptiveRateLimiter[F[_]] {

  /** Try to consume one request from the underlying rate limiter. */
  def consume: F[Boolean]

  /** Record one successful outcome from the protected sink. Safe to call from many fibers concurrently. */
  def recordSuccess: F[Unit]

  /** Record one failed outcome from the protected sink. Safe to call from many fibers concurrently. */
  def recordFailure: F[Unit]

  /** The AIMD's current estimate of the rate the protected sink can sustain. The underlying [[DynamicRateLimiter]] is
    * always configured at this same rate.
    */
  def rate: F[Rate]

  /** Most recently sampled failure ratio in `[0, 1]`; `0.0` until the measurement window is initialized. */
  def failureRatio: F[Double]
}

object AdaptiveRateLimiter {

  import FailureState._

  object syntax {

    implicit final class AdaptiveRateLimiterOps[F[_]](private val self: AdaptiveRateLimiter[F]) extends AnyVal {

      /** Try to consume one request and, on success, run `fa` while recording whether the result is a failure. When the
        * limiter is exhausted, return `orElse` without recording an outcome.
        */
      def protect[A](fa: F[A], isError: A => Boolean, orElse: => A)(implicit F: Monad[F]): F[A] =
        self.consume.flatMap { canProceed =>
          if (canProceed) fa.flatTap(a => if (isError(a)) self.recordFailure else self.recordSuccess)
          else orElse.pure[F]
        }

      /** Effectful variant of [[protect]]: the failure classifier itself returns `F[Boolean]`. */
      def protectF[A](fa: F[A], isError: A => F[Boolean], orElse: => F[A])(implicit F: Monad[F]): F[A] =
        self.consume.flatMap { canProceed =>
          if (canProceed)
            fa.flatTap(a => Applicative[F].ifF(isError(a))(ifTrue = self.recordFailure, ifFalse = self.recordSuccess))
          else orElse
        }
    }
  }

  /** Hysteresis band keyed on failure ratios. `start` is the failure ratio at which the band engages; `exit` is the
    * failure ratio at which the band releases. `start >= exit` keeps the band hysteretic and prevents flapping.
    */
  final case class HysteresisBand(exit: Double, start: Double) {
    def validate: Either[String, Unit] =
      Either.cond(start >= exit, (), "start >= exit to contain hysteresis")
  }

  /** Configuration for [[AdaptiveRateLimiter.start]].
    *
    * The rate-shaped fields ([[initialRate]], [[minRate]], [[maxRate]], [[rateIncreaseBy]]) all refer to the AIMD's
    * estimate of the protected sink's sustainable throughput, not to the bucket-refill mechanism (the underlying rate
    * limiter is configured at that estimate as a side effect).
    *
    * @param capacity
    *   maximum burst size of the underlying rate limiter
    * @param initialRate
    *   initial estimate of the sink's sustainable rate; AIMD starts here
    * @param minRate
    *   floor on the estimate; multiplicative decrease will not push below this
    * @param maxRate
    *   ceiling on the estimate; additive increase will not push above this
    * @param rateIncreaseBy
    *   additive AIMD increase rate
    * @param rateIncreasePeriod
    *   interval at which the additive increase is applied
    * @param rateDecreaseBy
    *   multiplicative AIMD step in `[0, 1]`: on a `Failing` signal, the estimate is shrunk to `(1 - rateDecrease) *
    *   current`
    * @param numberOfSlotsForMeasurements
    *   number of time-bucket slots in the failure-rate sliding window
    * @param slotDuration
    *   duration of each measurement slot
    * @param measurementPeriod
    *   how often the background fiber samples the LongAdder counters
    * @param minNumberOfMeasurements
    *   the sliding window must hold at least this many samples before the failure ratio is reported
    * @param failureLevels
    *   ordered (least- to most-severe) hysteresis bands keyed on failure ratio
    */
  final case class Config(
      capacity: Int,
      initialRate: Rate,
      minRate: Rate,
      maxRate: Rate,
      rateIncreaseBy: Rate,
      rateIncreasePeriod: FiniteDuration,
      rateDecreaseBy: Double,
      numberOfSlotsForMeasurements: Int,
      slotDuration: FiniteDuration,
      measurementPeriod: FiniteDuration,
      minNumberOfMeasurements: Int,
      failureLevels: NonEmptyList[HysteresisBand]
  ) {

    val rateLimiterConfig =
      RateLimiter.Config(capacity = capacity, refillRate = initialRate)

    val approximateFailureRatesConfig = ApproximateFailureRates.Config(
      numberOfSlotsForMeasurements = numberOfSlotsForMeasurements,
      slotDuration = slotDuration,
      measurementPeriod = measurementPeriod,
      minNumberOfMeasurements = minNumberOfMeasurements
    )

    val failureRateCategorizerConfig = FailureRateCategorizer.Config(
      failureLevels = failureLevels
    )

    val aimdRateControllerConfig = AimdRateController.Config(
      initialRate = initialRate,
      minRate = minRate,
      maxRate = maxRate,
      rateIncreaseBy = AimdRateController.AimdRateIncrease(
        rate = rateIncreaseBy,
        tickInterval = rateIncreasePeriod
      ),
      rateDecreaseBy = rateDecreaseBy
    )
  }

  object Config {

    /** Convenience builder for RPS-shaped configurations. Capacity is set to `maxRps`, the bucket window covers
      * `timeRangeForMeasurementInSeconds` one-second slots, and `minNumberOfMeasurements` equals the slot count.
      */
    def fromRps(
        minRps: Int,
        maxRps: Int,
        rpsIncreaseRate: Rate,
        rpsDecrease: Double,
        timeRangeForMeasurementInSeconds: Int,
        failureLevels: NonEmptyList[HysteresisBand]
    ): Config =
      Config(
        capacity = maxRps,
        initialRate = Rate(requests = maxRps, period = 1.second),
        minRate = Rate(requests = minRps, period = 1.second),
        maxRate = Rate(requests = maxRps, period = 1.second),
        rateIncreaseBy = rpsIncreaseRate,
        rateIncreasePeriod = rpsIncreaseRate.period,
        rateDecreaseBy = rpsDecrease,
        numberOfSlotsForMeasurements = timeRangeForMeasurementInSeconds,
        slotDuration = 1.second,
        measurementPeriod = 1.second,
        minNumberOfMeasurements = timeRangeForMeasurementInSeconds,
        failureLevels = failureLevels
      )
  }

  /** The categorizer's output. `Healthy` means the failure rate is below the least-severe band's `exit`. `Failing(N)`
    * means severity tier `N` (zero-indexed; higher is worse).
    */
  sealed abstract class FailureState extends Product with Serializable {
    def level: Int
  }

  object FailureState {

    case object Healthy extends FailureState {
      override val level: Int = -1
    }

    final case class Failing(level: Int) extends FailureState

    implicit val eq: Eq[FailureState] = Eq.fromUniversalEquals
  }

  private val NoSignal      = Chunk.empty[FailureState]
  private val HealthySignal = Chunk[FailureState](FailureState.Healthy)

  /** A no-op limiter that always allows consumption, never records outcomes, and reports a fixed sustainable rate. */
  def noop[F[_]: Applicative](rate: Rate = Rate(requests = 1, period = 1.second)): AdaptiveRateLimiter[F] =
    new NoopAdaptiveRateLimiter[F](configuredRate = rate)

  /** A test double that counts `recordSuccess` and `recordFailure` invocations and delegates `consume` to the supplied
    * effect.
    */
  def recording[F[_]: Sync](
      canConsume: F[Boolean],
      rate: Rate = Rate(requests = 1, period = 1.second)
  ): F[RecordingAdaptiveRateLimiter[F]] =
    (Ref[F].of(0), Ref[F].of(0)).mapN { (successes, failures) =>
      new RecordingAdaptiveRateLimiter[F](
        rate = rate,
        canConsume = canConsume,
        successes = successes,
        failures = failures
      )
    }

  def start[F[_]: Async](config: Config): Resource[F, AdaptiveRateLimiter[F]] =
    start[F](
      config = config,
      onFailureCategoryChange = (_: FailureState) => Async[F].unit,
      onRateChange = (_: Rate) => Async[F].unit
    )

  /** @param onFailureCategoryChange
    *   callback fired whenever the categorizer promotes or demotes the failure tier
    * @param onRateChange
    *   callback fired whenever the AIMD updates its estimated rate for the protected sink
    */
  def start[F[_]: Async](
      config: Config,
      onFailureCategoryChange: FailureState => F[Unit],
      onRateChange: Rate => F[Unit]
  ): Resource[F, AdaptiveRateLimiter[F]] =
    for {
      rateLimiter                  <- Resource.eval { RateLimiter.Dynamic[F](config = config.rateLimiterConfig) }
      failureRatesProducerConsumer <- Resource.eval {
        ApproximateFailureRates.createProducerAndConsumer(config = config.approximateFailureRatesConfig)
      }
      (measurements, failureRates) = failureRatesProducerConsumer
      failureRateCategorizer <- Resource.eval {
        FailureRateCategorizer[F](config = config.failureRateCategorizerConfig)
      }
      aimdRateController <- Resource.eval { AimdRateController[F](config = config.aimdRateControllerConfig) }

      adaptiveRateLimiter = new DefaultAdaptiveRateLimiter[F](rateLimiter = rateLimiter, measurements = measurements)

      _ <- Spawn[F].background {
        failureRates
          .evalTap(adaptiveRateLimiter.setFailureRatio)
          .through(failureRateCategorizer)
          .evalTap(onFailureCategoryChange)
          .through(aimdRateController)
          .evalTap(onRateChange)
          .evalMap(rateLimiter.setRefillRate)
          .compile
          .drain
      }
    } yield adaptiveRateLimiter

  private[adaptiveratelimiter] object ApproximateFailureRates {
    final case class Config(
        numberOfSlotsForMeasurements: Int,
        slotDuration: FiniteDuration,
        measurementPeriod: FiniteDuration,
        minNumberOfMeasurements: Int
    ) {

      def measurementWindow: FiniteDuration = slotDuration * numberOfSlotsForMeasurements

      def validate: Either[String, Unit] =
        for {
          _ <- check(numberOfSlotsForMeasurements > 0, "slots > 0")
          _ <- check(slotDuration > Duration.Zero, "slot duration > 0ms")
          _ <- check(measurementPeriod > Duration.Zero, "measurements period > 0ms")
          _ <- check(minNumberOfMeasurements > 0, "min measurements > 0")
        } yield ()
    }

    private[adaptiveratelimiter] def createProducerAndConsumer[F[_]: Async](
        config: Config
    ): F[(SampledMeasurements[F], fs2.Stream[F, Double])] =
      for {
        _            <- ApplicativeThrow[F].fromEither(config.validate.leftMap(new IllegalArgumentException(_)))
        measurements <- Measurements.sampledTimeBasedSlidingWindow[F](
          numberOfBuckets = config.numberOfSlotsForMeasurements,
          bucketSize = config.slotDuration,
          minNumberOfCalls = config.minNumberOfMeasurements
        )
      } yield (
        measurements,
        fs2.Stream
          .awakeEvery[F](period = config.measurementPeriod)
          .evalMap(_ => measurements.sample)
          .map(_.failureRate)
          .unNone
      )

  }

  private[adaptiveratelimiter] object FailureRateCategorizer {
    final case class Config(failureLevels: NonEmptyList[HysteresisBand]) {
      def validate: Either[String, Unit] =
        for {
          _ <- failureLevels.toList.zipWithIndex.traverse_ { case (band, idx) =>
            band.validate.leftMap(s => s"band[$idx]: $s")
          }
          _ <- failureLevels.toList
            .sliding(2)
            .collect { case Seq(a, b) => (a, b) }
            .toList
            .traverse_ { case (a, b) =>
              for {
                _ <- check(a.start < b.start, "bands must have monotonically increasing start")
                _ <- check(a.exit < b.exit, "bands must have monotonically increasing exit")
                _ <- check(a.start < b.exit, "bands must not overlap")
              } yield ()
            }
        } yield ()
    }

    def apply[F[_]: ApplicativeThrow](config: Config): F[Pipe[F, Double, FailureState]] =
      for {
        _ <- ApplicativeThrow[F].fromEither(config.validate.leftMap(new IllegalArgumentException(_)))
      } yield failureRateCategorizer(bands = config.failureLevels)

    private def failureRateCategorizer[F[_]](
        bands: NonEmptyList[HysteresisBand]
    ): Pipe[F, Double, FailureState] = {
      /*
      Avoid flapping around a single point and keep the output signal stable while the system adjusts to new rates.
      Producing too many unnecessary throttle signals would apply the multiplicative decrease each time, crushing
      throughput.
  
      Bands are ordered least-severe first (band 0 is the least severe). Promotions emit one signal per band crossed
      so consumers can step their AIMD response per tier. Demotions step one band at a time when the rate falls at or
      below the current band's `exit`, and only emit a signal on the final demotion to `Healthy`.
       */
      val bandsArr = bands.toList.toArray

      // TODO: this would be cleaner if it just emits state, then rate controller can track gradient.
      _.scan[(FailureState, Chunk[FailureState])]((FailureState.Healthy, NoSignal)) { case ((current, _), rate) =>
        val nextLevel    = bandsArr.lastIndexWhere(_.start <= rate)
        val currentLevel = current.level

        if (nextLevel > currentLevel) // worsening
          (
            FailureState.Failing(level = nextLevel),
            Chunk.from(
              (1 to nextLevel - currentLevel).map(step => FailureState.Failing(level = currentLevel + step))
            )
          )
        else
          current match {
            case FailureState.Failing(level) if rate <= bandsArr(level).exit =>
              // full recovery
              if (level == 0) (FailureState.Healthy, HealthySignal)
              // recovering
              else (FailureState.Failing(level = level - 1), NoSignal)
            // same or still within the hysteresis band
            case _ => (current, NoSignal)
          }
      }.collect { case (_, signals) => signals }.unchunks.changes
    }

  }

  private[adaptiveratelimiter] object AimdRateController {

    final case class AimdRateIncrease(rate: Rate, tickInterval: FiniteDuration)

    final case class Config(
        initialRate: Rate,
        minRate: Rate,
        maxRate: Rate,
        rateIncreaseBy: AimdRateIncrease,
        rateDecreaseBy: Double
    )

    private sealed abstract class Source extends Product with Serializable

    private object Source {
      case object Tick                                    extends Source
      final case class Signal(failureState: FailureState) extends Source
    }

    private[adaptiveratelimiter] def apply[F[_]: Temporal](
        config: AimdRateController.Config
    ): F[Pipe[F, FailureState, Rate]] =
      ApplicativeThrow[F]
        .fromEither {
          import config._
          (for {
            _ <- check(initialRate =!= Rate.Zero, "initialRate must be nonzero")
            _ <- check(minRate =!= Rate.Zero, "minRate must be nonzero")
            _ <- check(maxRate =!= Rate.Zero, "maxRate must be nonzero")
            _ <- check(rateIncreaseBy.rate =!= Rate.Zero, "rateIncreaseBy.rate must be nonzero")
            _ <- rateIncreaseBy.rate.validate.leftMap(_.getMessage)
            _ <- check(rateIncreaseBy.tickInterval > Duration.Zero, "rateIncreaseBy.tickInterval > 0ms")
            _ <- check(minRate <= initialRate && initialRate <= maxRate, "min rate <= initial rate <= max rate")
            _ <- check(rateDecreaseBy >= 0.0 && rateDecreaseBy <= 1.0, "0 <= rateDecreaseBy <= 1")
          } yield ())
            .leftMap(new IllegalArgumentException(_))
        }
        .as {
          val multiplicativeDecrease =
            (BigDecimal.valueOf(1.0) - BigDecimal.valueOf(config.rateDecreaseBy)).toDouble

          aimdRateController[F](
            initialRate = config.initialRate,
            minRate = config.minRate,
            maxRate = config.maxRate,
            rateIncreaseBy = config.rateIncreaseBy,
            multiplicativeDecrease = multiplicativeDecrease
          )
        }

    private def aimdRateController[F[_]: Temporal](
        initialRate: Rate,
        minRate: Rate,
        maxRate: Rate,
        rateIncreaseBy: AimdRateIncrease,
        multiplicativeDecrease: Double
    ): Pipe[F, FailureState, Rate] = { failureSignals =>
      // TODO: don't need to tick if we are max rate and healthy
      val ticks = fs2.Stream.awakeEvery[F](period = rateIncreaseBy.tickInterval).as(Source.Tick)

      failureSignals
        .map(Source.Signal(_))
        .merge(ticks)
        .scan(initialRate) {
          case (rate, Source.Signal(Healthy)) => rate
          case (rate, Source.Signal(_))       => rate.scaleBy(multiplicativeDecrease).max(minRate)
          case (rate, Source.Tick)            => (rate + rateIncreaseBy.rate).min(maxRate)
        }
    }
  }

  private def check(cond: Boolean, msg: String): Either[String, Unit] =
    Either.cond(cond, (), msg)

  @SuppressWarnings(Array("org.wartremover.warts.Var", "DisableSyntax.var"))
  private final class DefaultAdaptiveRateLimiter[F[_]: Sync](
      rateLimiter: DynamicRateLimiter[F],
      measurements: SampledMeasurements[F]
  ) extends AdaptiveRateLimiter[F] {

    @volatile private var _failureRatio: Double = 0.0

    override def consume: F[Boolean] = rateLimiter.consume()

    override def recordSuccess: F[Unit] = measurements.recordSuccess

    override def recordFailure: F[Unit] = measurements.recordFailure

    override def rate: F[Rate] = rateLimiter.refillRate

    override def failureRatio: F[Double] = Sync[F].delay(_failureRatio)

    def setFailureRatio(newValue: Double): F[Unit] = Sync[F].delay {
      _failureRatio = newValue
    }
  }
}

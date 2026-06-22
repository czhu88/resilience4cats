package io.mienks.resilience.admissioncontroller

import cats.effect.std.Random
import cats.effect.{Ref, Sync}
import cats.syntax.all._
import cats.{Applicative, ApplicativeThrow, Monad}
import io.mienks.resilience.Measurements.Snapshot
import io.mienks.resilience.{CountBasedSlidingWindowMeasurements, Measurements, TimeBasedSlidingWindowMeasurements}

import scala.concurrent.duration._
import scala.util.control.NoStackTrace

/** Admission control implementing the SRE adaptive throttling pattern.
  *
  * Acts as a client-side brake to shed load when the downstream is overloaded. On each [[allow]] the controller draws a
  * probabilistic admission decision from the recent outcomes of admitted calls, tracked in a sliding window. Admitting
  * probabilistically (rather than a hard open/closed gate) keeps the controller probing the downstream even while
  * shedding, so it adapts as the downstream's load and capacity change. Local rejections are recorded as failures so
  * they count as non-accepts and the window self-stabilizes.
  *
  * Each request is rejected with probability `max(0, (requests - k * accepts) / (requests + 1))` over the window. The
  * `k` parameter tunes how aggressively load is shed: higher `k` sheds less (more permissive), lower `k` sheds more. At
  * `k = 1` this reduces to shedding at the plain failure ratio, but `k = 1` is only marginally stable, so once the
  * controller starts shedding it can stay latched even after the downstream recovers. Choosing `k > 1` (default `2.0`)
  * adds a restoring force that guarantees the gate fully reopens once the downstream is healthy, and creates a dead
  * zone so transient failures do not trigger shedding. See [[AdmissionController.Config]].
  *
  * Credits: https://sre.google/sre-book/handling-overload/
  */
trait AdmissionController[F[_]] {

  def allow: F[Boolean]

  def record(isFailure: Boolean): F[Snapshot]

  def rejectionProbability: F[Double]
}

object AdmissionController {

  object syntax {

    implicit final class AdmissionControllerOps[F[_]](private val self: AdmissionController[F]) extends AnyVal {

      /** Gate `fa`, recording its classified outcome when admitted. Local rejections are recorded as failures. */
      def protect[A](fa: F[A], isFailure: A => Boolean, orElse: => A)(implicit F: Monad[F]): F[A] =
        self.allow.flatMap {
          case true  => fa.flatTap(a => self.record(isFailure = isFailure(a)).void)
          case false => self.record(isFailure = true).as(orElse)
        }

      /** Effectful variant of [[protect]]: the failure classifier itself returns `F[Boolean]`. */
      def protectF[A](fa: F[A], isFailure: A => F[Boolean], orElse: => F[A])(implicit F: Monad[F]): F[A] =
        self.allow.flatMap {
          case true  => fa.flatTap(a => isFailure(a).flatMap(outcome => self.record(isFailure = outcome).void))
          case false => self.record(isFailure = true) >> orElse
        }
    }
  }

  sealed trait MeasurementStrategy extends Product with Serializable

  object MeasurementStrategy {

    final case class CountBasedSlidingWindow(windowSize: Int, minNumberOfCalls: Int) extends MeasurementStrategy

    final case class TimeBasedSlidingWindow(
        numberOfBuckets: Int,
        bucketSize: FiniteDuration,
        minNumberOfCalls: Int
    ) extends MeasurementStrategy
  }

  final case class Config(
      measurementStrategy: MeasurementStrategy =
        MeasurementStrategy.CountBasedSlidingWindow(windowSize = 100, minNumberOfCalls = 10),
      k: Double = 2.0
  ) {

    def validate: Either[Throwable, Unit] =
      for {
        _ <- check(k >= 1.0, s"k must be at least 1.0, got: $k")
        _ <- measurementStrategy match {
          case MeasurementStrategy.CountBasedSlidingWindow(windowSize, minNumberOfCalls) =>
            for {
              _ <- check(windowSize > 0, s"windowSize must be positive, got: $windowSize")
              _ <- check(minNumberOfCalls > 0, s"minNumberOfCalls must be positive, got: $minNumberOfCalls")
            } yield ()
          case MeasurementStrategy.TimeBasedSlidingWindow(numberOfBuckets, bucketSize, minNumberOfCalls) =>
            for {
              _ <- check(numberOfBuckets > 0, s"numberOfBuckets must be positive, got: $numberOfBuckets")
              _ <- check(bucketSize > Duration.Zero, s"bucketSize must be positive, got: $bucketSize")
              _ <- check(minNumberOfCalls > 0, s"minNumberOfCalls must be positive, got: $minNumberOfCalls")
            } yield ()
        }
      } yield ()
  }

  /** Build an [[AdmissionController]] using `ThreadLocalRandom` for the admission draw. See [[AdmissionController]] for
    * the shedding behavior and how `k` tunes it.
    *
    * @param config
    *   sliding-window measurement strategy and shedding aggressiveness `k`
    */
  def apply[F[_]: Sync](config: Config = Config()): F[AdmissionController[F]] =
    apply(config = config, random = Random.javaUtilConcurrentThreadLocalRandom[F])

  /** Build an [[AdmissionController]] with an explicit [[Random]] (e.g. a seeded instance for deterministic tests),
    * validating `config` first. See [[AdmissionController]] for the shedding behavior and how `k` tunes it.
    *
    * @param config
    *   sliding-window measurement strategy and shedding aggressiveness `k`
    * @param random
    *   source for the probabilistic admission decision
    */
  def apply[F[_]: Sync](config: Config, random: Random[F]): F[AdmissionController[F]] =
    for {
      _          <- ApplicativeThrow[F].fromEither(config.validate)
      controller <- unsafe(config = config, random = random)
    } yield controller

  def unsafe[F[_]: Sync](config: Config, random: Random[F]): F[AdmissionController[F]] =
    for {
      measurements <- config.measurementStrategy match {
        case MeasurementStrategy.CountBasedSlidingWindow(windowSize, minNumberOfCalls) =>
          CountBasedSlidingWindowMeasurements[F](
            windowSize = windowSize,
            minNumberOfCalls = minNumberOfCalls
          ).widen

        case MeasurementStrategy.TimeBasedSlidingWindow(numberOfBuckets, bucketSize, minNumberOfCalls) =>
          TimeBasedSlidingWindowMeasurements[F](
            numberOfBuckets = numberOfBuckets,
            bucketSize = bucketSize,
            minNumberOfCalls = minNumberOfCalls
          ).widen
      }
    } yield new Default[F](measurements = measurements, random = random, k = config.k)

  def noop[F[_]: Applicative]: AdmissionController[F] =
    new NoopAdmissionController[F]

  def recording[F[_]: Sync](canAllow: F[Boolean]): F[RecordingAdmissionController[F]] =
    Ref[F]
      .of(RecordingAdmissionController.State.empty)
      .map(state => new RecordingAdmissionController[F](canAllow = canAllow, state = state))

  private[admissioncontroller] def rejectionProbability(k: Double)(snapshot: Snapshot): Double =
    if (!snapshot.isInitialized) 0.0
    else {
      val accepts = snapshot.totalMeasurements - snapshot.totalFailures
      math.max(0.0, (snapshot.totalMeasurements - k * accepts) / (snapshot.totalMeasurements + 1.0))
    }

  private def check(cond: Boolean, msg: String): Either[Throwable, Unit] =
    Either.cond(cond, (), new IllegalArgumentException(msg) with NoStackTrace)

  private final class Default[F[_]: Monad](
      measurements: Measurements[F],
      random: Random[F],
      k: Double
  ) extends AdmissionController[F] {

    private val rejectProbability = AdmissionController.rejectionProbability(k = k)(_)

    override def allow: F[Boolean] =
      measurements.peek.flatMap { snapshot =>
        random.nextDouble.map(_ >= rejectProbability(snapshot))
      }

    override def record(isFailure: Boolean): F[Snapshot] =
      measurements.record(isFailure = isFailure)

    override def rejectionProbability: F[Double] =
      measurements.peek.map(rejectProbability)
  }
}

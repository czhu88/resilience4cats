package io.mienks.resilience

import cats.effect.{Clock, Ref, Sync}
import cats.kernel.Eq
import cats.syntax.all._
import Measurements.Snapshot

import java.util.concurrent.atomic.LongAdder
import scala.collection.immutable
import scala.concurrent.duration._

trait Measurements[F[_]] {

  def record(isFailure: Boolean): F[Snapshot]

  def peek: F[Snapshot]

  def reset: F[Unit]
}

/** Counter-backed measurements where recording and sampling happen on different cadences.
  *
  * `recordSuccess` and `recordFailure` are safe to call from many fibers concurrently. `sample` must be called from a
  * single fiber because the bucket window state is intentionally mutable.
  */
trait SampledMeasurements[F[_]] {

  def recordSuccess: F[Unit]

  def recordFailure: F[Unit]

  def sample: F[Snapshot]
}

object Measurements {

  def countBasedSlidingWindow[F[_]: Sync](
      windowSize: Int,
      minNumberOfCalls: Int
  ): F[CountBasedSlidingWindowMeasurements[F]] =
    CountBasedSlidingWindowMeasurements[F](windowSize, minNumberOfCalls)

  def timeBasedSlidingWindow[F[_]: Sync](
      numberOfBuckets: Int,
      bucketSize: FiniteDuration,
      minNumberOfCalls: Int
  ): F[TimeBasedSlidingWindowMeasurements[F]] =
    TimeBasedSlidingWindowMeasurements(numberOfBuckets, bucketSize, minNumberOfCalls)

  /** Approximates failure rate over a sliding time window.
    *
    * `recordSuccess` and `recordFailure` may be called concurrently. `sample` is NOT thread safe and intended for a
    * single consumer.
    *
    * windowSize (duration) = numberOfBuckets * bucketSize
    *
    * @param numberOfBuckets
    *   the number of intervals that make up the window size
    * @param bucketSize
    *   the duration of each interval
    * @param minNumberOfCalls
    *   the minimum number of recorded calls required before the failure rate is considered initialized and valid;
    *   recordings below this value indicate that the algorithm doesn't know the real failure rate.
    * @return
    */
  def sampledTimeBasedSlidingWindow[F[_]: Sync](
      numberOfBuckets: Int,
      bucketSize: FiniteDuration,
      minNumberOfCalls: Int
  ): F[SampledMeasurements[F]] =
    SampledTimeBasedSlidingWindowMeasurements(numberOfBuckets, bucketSize, minNumberOfCalls).widen

  /** A point-in-time view of the sliding window's aggregate counters.
    *
    * @param totalMeasurements
    *   the number of recorded outcomes currently in the window
    * @param totalFailures
    *   the number of recorded failures currently in the window
    * @param isInitialized
    *   whether the window has enough measurements to evaluate rates
    */
  final case class Snapshot(totalMeasurements: Int, totalFailures: Int, isInitialized: Boolean) {

    /** The ratio of failures to total measurements, or `None` when [[isInitialized]] is false. */
    def failureRate: Option[Double] =
      Option.when(isInitialized)(totalFailures.toDouble / totalMeasurements)
  }

  object Snapshot {
    implicit val eq: Eq[Snapshot] = Eq.fromUniversalEquals
  }
}

/** Approximates failure rate over a sliding time window.
  *
  * `recordSuccess` and `recordFailure` may be called concurrently. `sample` is NOT thread safe and intended for a
  * single consumer.
  */
final class SampledTimeBasedSlidingWindowMeasurements[F[_]: Sync] private (
    successCount: LongAdder,
    failureCount: LongAdder,
    numberOfBuckets: Int,
    bucketLengthInNanos: Long,
    minNumberOfCalls: Int,
    createdAt: Long
) extends SampledMeasurements[F] {

  private val buckets =
    Array.fill(numberOfBuckets)(SampledTimeBasedSlidingWindowMeasurements.TimeBucket.empty(createdAt = createdAt))
  private var index             = 0
  private var totalMeasurements = 0L
  private var totalFailures     = 0L
  private var lastSuccessCount  = 0L
  private var lastFailureCount  = 0L

  override def recordSuccess: F[Unit] =
    Sync[F].delay(successCount.increment())

  override def recordFailure: F[Unit] =
    Sync[F].delay(failureCount.increment())

  override def sample: F[Snapshot] =
    Clock[F].monotonic.map(_.toNanos).flatMap { now =>
      Sync[F].delay(
        sample(now = now, currentSuccessCount = successCount.sum(), currentFailureCount = failureCount.sum())
      )
    }

  private def sample(now: Long, currentSuccessCount: Long, currentFailureCount: Long): Snapshot = {
    val successDelta = currentSuccessCount - lastSuccessCount
    val failureDelta = currentFailureCount - lastFailureCount

    lastSuccessCount = currentSuccessCount
    lastFailureCount = currentFailureCount

    val timeBucketsSinceLastUpdate = (now - buckets(index).createdAt) / bucketLengthInNanos
    if (timeBucketsSinceLastUpdate > 0L) {
      var bucketsToMoveBy = math.min(timeBucketsSinceLastUpdate, numberOfBuckets.toLong)
      do {
        bucketsToMoveBy -= 1L
        index = (index + 1) % numberOfBuckets
        totalMeasurements -= buckets(index).total
        totalFailures -= buckets(index).failures
        buckets(index).reset(newCreatedAt = now)
      } while (bucketsToMoveBy > 0L)
    }

    buckets(index).add(successes = successDelta, newFailures = failureDelta)
    totalMeasurements += successDelta + failureDelta
    totalFailures += failureDelta

    Snapshot(
      totalMeasurements = totalMeasurements.toInt,
      totalFailures = totalFailures.toInt,
      isInitialized = totalMeasurements >= minNumberOfCalls.toLong
    )
  }
}

object SampledTimeBasedSlidingWindowMeasurements {

  def apply[F[_]: Sync](
      numberOfBuckets: Int,
      bucketSize: FiniteDuration,
      minNumberOfCalls: Int
  ): F[SampledTimeBasedSlidingWindowMeasurements[F]] =
    for {
      _            <- Sync[F].delay(require(numberOfBuckets > 0, "numberOfBuckets > 0"))
      _            <- Sync[F].delay(require(bucketSize >= 10.milliseconds, "bucketSize >= 10.milliseconds"))
      now          <- Clock[F].monotonic
      measurements <- Sync[F].delay {
        new SampledTimeBasedSlidingWindowMeasurements[F](
          successCount = new LongAdder(),
          failureCount = new LongAdder(),
          numberOfBuckets = numberOfBuckets,
          bucketLengthInNanos = bucketSize.toNanos,
          minNumberOfCalls = minNumberOfCalls,
          createdAt = now.toNanos
        )
      }
    } yield measurements

  private final class TimeBucket(var createdAt: Long, var failures: Long, var total: Long) {

    def reset(newCreatedAt: Long): Unit = {
      createdAt = newCreatedAt
      failures = 0L
      total = 0L
    }

    def add(successes: Long, newFailures: Long): Unit = {
      failures += newFailures
      total += successes + newFailures
    }
  }

  private object TimeBucket {
    def empty(createdAt: Long): TimeBucket = new TimeBucket(createdAt, failures = 0L, total = 0L)
  }

}

final class CountBasedSlidingWindowMeasurements[F[_]: Sync] private (
    stateRef: Ref[F, CountBasedSlidingWindowMeasurements.State]
) extends Measurements[F] {

  override def record(isFailure: Boolean): F[Snapshot] =
    stateRef.modify(_.record(isFailure))

  override def peek: F[Snapshot] =
    stateRef.get.map(_.snapshot)

  override def reset: F[Unit] =
    stateRef.update(_.reset)
}

object CountBasedSlidingWindowMeasurements {

  def apply[F[_]: Sync](windowSize: Int, minNumberOfCalls: Int): F[CountBasedSlidingWindowMeasurements[F]] =
    Ref[F]
      .of(State.empty(windowSize, minNumberOfCalls))
      .map(new CountBasedSlidingWindowMeasurements[F](_))

  private[resilience] final case class State(
      failures: immutable.BitSet,
      index: Int,
      windowSize: Int,
      totalMeasurements: Int,
      totalFailures: Int,
      minNumberOfCalls: Int
  ) {

    def record(isFailure: Boolean): (State, Snapshot) = {
      val newTotalMeasurements = math.min(totalMeasurements + 1, windowSize)
      val newIndex             = (index + 1) % windowSize
      val evictedWasFailure    = failures.contains(newIndex)
      val adjustedFailures     = if (evictedWasFailure) totalFailures - 1 else totalFailures
      val newFailures          = if (isFailure) adjustedFailures + 1 else adjustedFailures
      val newBits              = if (isFailure) failures + newIndex else failures - newIndex
      (
        copy(
          failures = newBits,
          index = newIndex,
          totalMeasurements = newTotalMeasurements,
          totalFailures = newFailures
        ),
        copy(totalMeasurements = newTotalMeasurements, totalFailures = newFailures).snapshot
      )
    }

    def snapshot: Snapshot =
      Snapshot(
        totalMeasurements = totalMeasurements,
        totalFailures = totalFailures,
        isInitialized = totalMeasurements >= minNumberOfCalls
      )

    def reset: State = State.empty(windowSize, minNumberOfCalls)
  }

  private[resilience] object State {

    def empty(windowSize: Int, minNumberOfCalls: Int): State = State(
      failures = immutable.BitSet.empty,
      index = 0,
      windowSize = windowSize,
      totalMeasurements = 0,
      totalFailures = 0,
      minNumberOfCalls = minNumberOfCalls
    )
  }
}

final class TimeBasedSlidingWindowMeasurements[F[_]: Sync] private (
    stateRef: Ref[F, TimeBasedSlidingWindowMeasurements.State]
) extends Measurements[F] {

  override def record(isFailure: Boolean): F[Snapshot] =
    Clock[F].monotonic.map(_.toNanos).flatMap { now =>
      stateRef.modify(_.record(isFailure, now))
    }

  override def peek: F[Snapshot] =
    Clock[F].monotonic.map(_.toNanos).flatMap { now =>
      stateRef.modify { state =>
        val advanced = state.advance(now = now)
        (advanced, advanced.snapshot)
      }
    }

  override def reset: F[Unit] =
    Clock[F].monotonic.map(_.toNanos).flatMap { now =>
      stateRef.update(_.reset(now))
    }
}

object TimeBasedSlidingWindowMeasurements {

  def apply[F[_]: Sync](
      numberOfBuckets: Int,
      bucketSize: FiniteDuration,
      minNumberOfCalls: Int
  ): F[TimeBasedSlidingWindowMeasurements[F]] =
    for {
      _        <- Sync[F].delay(require(numberOfBuckets > 0, "numberOfBuckets > 0"))
      _        <- Sync[F].delay(require(bucketSize >= 10.milliseconds, "bucketSize >= 10.milliseconds"))
      now      <- Clock[F].monotonic
      stateRef <- Ref[F].of(
        State.initial(
          numberOfBuckets = numberOfBuckets,
          bucketLengthInNanos = bucketSize.toNanos,
          minNumberOfCalls = minNumberOfCalls,
          createdAt = now.toNanos
        )
      )
    } yield new TimeBasedSlidingWindowMeasurements[F](stateRef)

  private[resilience] final case class TimeBucket(createdAt: Long, failures: Int, total: Int) {
    def addMeasurement(isFailure: Boolean): TimeBucket =
      copy(failures = failures + (if (isFailure) 1 else 0), total = total + 1)
  }

  private object TimeBucket {
    def empty(createdAt: Long): TimeBucket = TimeBucket(createdAt, failures = 0, total = 0)
  }

  private[resilience] final case class State(
      buckets: Vector[TimeBucket],
      index: Int,
      numberOfBuckets: Int,
      bucketLengthInNanos: Long,
      totalMeasurements: Int,
      totalFailures: Int,
      minNumberOfCalls: Int
  ) {

    def record(isFailure: Boolean, now: Long): (State, Snapshot) = {
      val advanced  = advance(now)
      val curBucket = advanced.buckets(advanced.index).addMeasurement(isFailure)
      val updated   = advanced.copy(
        buckets = advanced.buckets.updated(advanced.index, curBucket),
        totalMeasurements = advanced.totalMeasurements + 1,
        totalFailures = advanced.totalFailures + (if (isFailure) 1 else 0)
      )

      (updated, updated.snapshot)
    }

    def advance(now: Long): State = {
      val timeBucketsSinceLastUpdate = (now - buckets(index).createdAt) / bucketLengthInNanos

      var curIndex             = index
      var curTotalMeasurements = totalMeasurements
      var curTotalFailures     = totalFailures
      var curBuckets           = buckets

      if (timeBucketsSinceLastUpdate > 0) {
        var bucketsToMoveBy = math.min(timeBucketsSinceLastUpdate, numberOfBuckets.toLong)
        do {
          bucketsToMoveBy -= 1L
          curIndex = (curIndex + 1) % numberOfBuckets
          val bucket = curBuckets(curIndex)
          curTotalMeasurements -= bucket.total
          curTotalFailures -= bucket.failures
          curBuckets = curBuckets.updated(curIndex, TimeBucket.empty(createdAt = now))
        } while (bucketsToMoveBy > 0)
      }

      copy(
        buckets = curBuckets,
        index = curIndex,
        totalMeasurements = curTotalMeasurements,
        totalFailures = curTotalFailures
      )
    }

    def snapshot: Snapshot =
      Snapshot(
        totalMeasurements = totalMeasurements,
        totalFailures = totalFailures,
        isInitialized = totalMeasurements >= minNumberOfCalls
      )

    def reset(now: Long): State =
      State.initial(numberOfBuckets, bucketLengthInNanos, minNumberOfCalls, now)
  }

  private[resilience] object State {

    def initial(
        numberOfBuckets: Int,
        bucketLengthInNanos: Long,
        minNumberOfCalls: Int,
        createdAt: Long
    ): State = State(
      buckets = Vector.fill(numberOfBuckets)(TimeBucket.empty(createdAt = createdAt)),
      index = 0,
      numberOfBuckets = numberOfBuckets,
      bucketLengthInNanos = bucketLengthInNanos,
      totalMeasurements = 0,
      totalFailures = 0,
      minNumberOfCalls = minNumberOfCalls
    )
  }
}

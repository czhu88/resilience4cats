package io.mienks.resilience.charts

import cats.data.NonEmptyList
import cats.effect.{IO, Ref, Resource}
import cats.syntax.all._
import io.mienks.resilience.Rate
import io.mienks.resilience.ratelimiter.{DynamicRateLimiter, RateLimiter}

import scala.concurrent.duration.FiniteDuration
import scala.util.Random

/** A simulated downstream resource whose health is a function of the offered load.
  *
  * A phase is a hard ceiling plus zero or more soft ceilings. The hard ceiling is the base sustainable rate: any call
  * admitted above it fails outright (100%). Each [[Backend.SoftCeiling]] is a tighter rate that fails only a fraction
  * (`failProbability`) of the calls that overflow it. Every ceiling is a token bucket ([[DynamicRateLimiter]]); a call
  * consumes one token from each bucket and fails with the most severe `failProbability` among the ceilings it
  * overflowed (the hard ceiling contributing 1.0). This makes the observed failure ratio an emergent property of the
  * rate the adaptive limiter admits, closing the control loop.
  */
trait Backend[F[_]] {

  /** Issue one call. `true` = success, `false` = overload failure. */
  def call: F[Boolean]

  /** The active phase's hard ceiling (the bottleneck the limiter is trying to discover). */
  def baseCapacity: F[Rate]
}

object Backend {

  /** A ceiling below the hard ceiling that fails only `failProbability` of the calls that overflow it. */
  final case class SoftCeiling(capacity: Rate, failProbability: Double) {
    require(
      failProbability >= 0.0 && failProbability < 1.0,
      s"SoftCeiling.failProbability must be in [0.0, 1.0), got: ${failProbability.toString}"
    )
  }

  /** One leg of a backend schedule: hold the backend at `hardCeiling` (plus `softCeilings`) for `duration`. */
  final case class Phase(hardCeiling: Rate, softCeilings: List[SoftCeiling], duration: FiniteDuration)

  // Burst tokens per ceiling; kept small so each limiter behaves like a rate ceiling rather than a buffer.
  private val BurstCapacity: Int = 8

  /** Build a backend that walks `schedule` on an internal fiber, reconfiguring its ceilings as each phase begins. All
    * phases must share the same number of soft ceilings, since the buckets are created once from the head phase.
    */
  def create(schedule: NonEmptyList[Phase], seed: Long): Resource[IO, Backend[IO]] = {
    val head = schedule.head
    for {
      rng                  <- Resource.eval(IO(new Random(seed)))
      hard                 <- Resource.eval(bucket(head.hardCeiling))
      softs                <- Resource.eval(head.softCeilings.traverse(ceiling => bucket(ceiling.capacity)))
      failProbabilitiesRef <- Resource.eval(Ref[IO].of(head.softCeilings.map(_.failProbability)))
      baseRef              <- Resource.eval(Ref[IO].of(head.hardCeiling))
      _                    <- drive(schedule, hard, softs, failProbabilitiesRef, baseRef).background
    } yield new TokenBucketBackend(
      hard = hard,
      softs = softs,
      failProbabilitiesRef = failProbabilitiesRef,
      baseRef = baseRef,
      rng = rng
    )
  }

  private def bucket(refillRate: Rate): IO[DynamicRateLimiter[IO]] =
    RateLimiter.Dynamic.full[IO](capacity = BurstCapacity, refillRate = refillRate)

  private def drive(
      schedule: NonEmptyList[Phase],
      hard: DynamicRateLimiter[IO],
      softs: List[DynamicRateLimiter[IO]],
      failProbabilitiesRef: Ref[IO, List[Double]],
      baseRef: Ref[IO, Rate]
  ): IO[Unit] =
    schedule.traverse_ { phase =>
      hard.setRefillRate(phase.hardCeiling) >>
        softs.zip(phase.softCeilings).traverse_ { case (limiter, ceiling) =>
          limiter.setRefillRate(ceiling.capacity)
        } >>
        failProbabilitiesRef.set(phase.softCeilings.map(_.failProbability)) >>
        baseRef.set(phase.hardCeiling) >>
        IO.sleep(phase.duration)
    }

  private final class TokenBucketBackend(
      hard: DynamicRateLimiter[IO],
      softs: List[DynamicRateLimiter[IO]],
      failProbabilitiesRef: Ref[IO, List[Double]],
      baseRef: Ref[IO, Rate],
      rng: Random
  ) extends Backend[IO] {

    override def call: IO[Boolean] =
      for {
        hardAdmitted      <- hard.consume()
        softAdmitted      <- softs.traverse(_.consume())
        failProbabilities <- failProbabilitiesRef.get
        overflowed      = softAdmitted.zip(failProbabilities).collect { case (admitted, p) if !admitted => p }
        failProbability = if (!hardAdmitted) 1.0 else overflowed.maxOption.getOrElse(0.0)
        result <- if (failProbability <= 0.0) true.pure[IO] else IO(rng.nextDouble()).map(_ >= failProbability)
      } yield result

    override def baseCapacity: IO[Rate] = baseRef.get
  }
}

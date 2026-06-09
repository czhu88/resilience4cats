package io.mienks.resilience.charts

import cats.data.NonEmptyList
import cats.effect.IO
import cats.syntax.all._
import io.mienks.resilience.Rate
import io.mienks.resilience.ratelimiter.{DynamicRateLimiter, RateLimiter}

import scala.util.Random

/** A simulated downstream resource whose health is a function of the offered load.
  *
  * Each tier is a token bucket ([[DynamicRateLimiter]]) sized at a multiple of the backend's base capacity. A call
  * consumes one token from every tier; the call's failure probability is the most severe `failProbability` among the
  * tiers whose bucket was empty (i.e. whose capacity the offered load exceeded). This makes the observed failure ratio
  * an emergent property of the rate the adaptive limiter admits, closing the control loop.
  */
trait Backend[F[_]] {

  /** Issue one call. `true` = success, `false` = overload failure. */
  def call: F[Boolean]

  /** Reconfigure the backend's base sustainable capacity; tiers are rescaled relative to it. */
  def setCapacity(base: Rate): F[Unit]
}

object Backend {

  /** One capacity tier. `relativeCapacity` scales the backend's base capacity; `failProbability` is the chance a call
    * fails once the offered load overflows this tier's bucket.
    */
  final case class Tier(relativeCapacity: Double, failProbability: Double)

  /** Default backend: a single bucket at the base capacity whose overflow always fails (`failureRatio = 1 - C/A`). */
  val Single: NonEmptyList[Tier] = NonEmptyList.one(Tier(relativeCapacity = 1.0, failProbability = 1.0))

  // Burst tokens per tier; kept small so each limiter behaves like a rate ceiling rather than a buffer.
  private val BurstCapacity: Int = 8

  def create(base: Rate, tiers: NonEmptyList[Tier], seed: Long): IO[Backend[IO]] =
    for {
      rng      <- IO(new Random(seed))
      limiters <- tiers.traverse { tier =>
        RateLimiter.Dynamic
          .full[IO](capacity = BurstCapacity, refillRate = base.scaleBy(tier.relativeCapacity))
          .map(limiter => (tier, limiter))
      }
    } yield new TieredBackend(tiers = limiters, rng = rng)

  private final class TieredBackend(
      tiers: NonEmptyList[(Tier, DynamicRateLimiter[IO])],
      rng: Random
  ) extends Backend[IO] {

    override def call: IO[Boolean] =
      tiers
        .traverse { case (tier, limiter) =>
          limiter.consume().map(admitted => if (admitted) 0.0 else tier.failProbability)
        }
        .flatMap { failProbabilities =>
          val failProbability = failProbabilities.maximum
          if (failProbability <= 0.0) true.pure[IO]
          else IO(rng.nextDouble()).map(_ >= failProbability)
        }

    override def setCapacity(base: Rate): IO[Unit] =
      tiers.traverse_ { case (tier, limiter) => limiter.setRefillRate(base.scaleBy(tier.relativeCapacity)) }
  }
}

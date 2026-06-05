package io.mienks.resilience.adaptiveratelimiter

import cats.effect.{Ref, Sync}
import io.mienks.resilience.Rate

import scala.concurrent.duration._

/** Test double for [[AdaptiveRateLimiter]] that counts `recordSuccess` and `recordFailure` invocations and delegates
  * `consume` to the supplied effect. Reports a fixed [[Rate]] like [[NoopAdaptiveRateLimiter]].
  */
class RecordingAdaptiveRateLimiter[F[_]: Sync] private[adaptiveratelimiter] (
    rate: Rate = Rate(requests = 1, period = 1.second),
    canConsume: F[Boolean],
    val successes: Ref[F, Int],
    val failures: Ref[F, Int]
) extends NoopAdaptiveRateLimiter[F](configuredRate = rate) {

  override def consume: F[Boolean] = canConsume

  override def recordSuccess: F[Unit] = successes.update(_ + 1)

  override def recordFailure: F[Unit] = failures.update(_ + 1)
}

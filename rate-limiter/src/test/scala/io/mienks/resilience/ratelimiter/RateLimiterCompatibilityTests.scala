package io.mienks.resilience.ratelimiter

import cats.syntax.option._
import io.mienks.resilience.ratelimiter.RateLimiter.RefillRate
import munit.FunSuite

import scala.concurrent.duration._

final class RateLimiterCompatibilityTests extends FunSuite {

  test("RefillRate aliases remain source-compatible") {
    import RateLimiter.RefillRate.parse
    import RateLimiter.syntax._

    assertEquals(1.per(1.second), RefillRate(1, 1.second))
    assertEquals(parse("8 requests / 2 minutes"), RefillRate(8, 2.minutes).some)
    assertEquals(rate"8 requests / 2 minutes", RefillRate(8, 2.minutes))
  }
}

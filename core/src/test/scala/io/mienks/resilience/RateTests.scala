package io.mienks.resilience

import cats.kernel.{Monoid, Order}
import cats.syntax.eq._
import cats.syntax.option._
import cats.syntax.semigroup._
import io.mienks.resilience.Rate.syntax._
import munit.FunSuite

import scala.concurrent.duration._

final class RateTests extends FunSuite {

  test("Eq matches throughput") {
    assert(Rate(1, 1.second) === Rate(60, 1.minute))
    assert(Rate(1, 1.second) =!= Rate(2, 1.second))

    assert(Rate(requests = 1, period = 30.seconds) === Rate(requests = 2, period = 1.minute))
  }

  test("Monoid empty is zero; combine sums effective rates") {
    val onePerSecond = Rate(1, 1.second)
    assert(Monoid[Rate].empty === Rate.Zero)
    assert((Rate.Zero |+| onePerSecond) === onePerSecond)
    assert((onePerSecond |+| Rate.Zero) === onePerSecond)
    assert((onePerSecond |+| onePerSecond) === Rate(2, 1.second))
    assert((onePerSecond |+| onePerSecond) === 2.per(1.second))
    assertEquals(60.per(1.minute) |+| onePerSecond, 120.per(1.minute))
    assertEquals(onePerSecond |+| 60.per(1.minute), 2.per(1.second))
    assertEquals(Int.MaxValue.per(1.second) |+| Int.MaxValue.per(1.second), Int.MaxValue.per(500.millis))
  }

  test("Monoid combine is associative") {
    val a = Rate(1, 1.second)
    val b = Rate(2, 1.second)
    val c = Rate(1, 2.seconds)
    assert(((a |+| b) |+| c) === (a |+| (b |+| c)))
  }

  test("parse accepts valid rates and rejects invalid rates") {
    assertEquals(Rate.parse("8 requests / 2 minutes"), Rate(8, 2.minutes).some)
    assertEquals(Rate.parse("500 requests / 4 hours"), Rate(500, 4.hours).some)
    assertEquals(Rate.parse("abc requests / 4 hours"), none[Rate])
    assertEquals(Rate.parse("500 requests / xyz hours"), none[Rate])
  }

  test("rate syntax creates rates and rejects invalid input") {
    assertEquals(1.per(1.second), Rate(1, 1.second))
    assertEquals(12.per(6.seconds), Rate(12, 6.seconds))

    assertEquals(rate"8 requests / 2 minutes", Rate(8, 2.minutes))
    assertEquals(rate"500 requests / 4 hours", Rate(500, 4.hours))
    intercept[IllegalArgumentException](rate"abc requests / 4 hours")
    intercept[NumberFormatException](rate"500 requests / xyz hours")
  }

  test("compare orders by effective throughput") {
    val onePerSecond   = Rate(1, 1.second)
    val sixtyPerMinute = Rate(60, 1.minute)
    assertEquals(onePerSecond.compare(sixtyPerMinute), 0)

    val slower = Rate(1, 2.seconds)
    val faster = Rate(1, 1.second)
    assert(slower < faster)
    assert(faster > slower)

    val twoPerSecond = 2.per(1.second)
    assert(onePerSecond < twoPerSecond)
    assert(twoPerSecond.compare(onePerSecond) > 0)
    assertEquals(Order[Rate].compare(x = onePerSecond, y = twoPerSecond), onePerSecond.compare(twoPerSecond))
  }

  test("min and max compare by effective throughput") {
    val onePerSecond    = Rate(1, 1.second)
    val sixtyPerMinute  = Rate(60, 1.minute)
    val thirtyPerMinute = Rate(30, 1.minute)
    val twoPerSecond    = Rate(2, 1.second)

    assertEquals(onePerSecond.min(that = thirtyPerMinute), thirtyPerMinute)
    assertEquals(thirtyPerMinute.max(that = onePerSecond), onePerSecond)
    assertEquals(onePerSecond.max(that = twoPerSecond), twoPerSecond)
    assertEquals(twoPerSecond.min(that = onePerSecond), onePerSecond)
    assertEquals(onePerSecond.min(that = sixtyPerMinute), onePerSecond)
    assertEquals(sixtyPerMinute.max(that = onePerSecond), sixtyPerMinute)
  }

  test("subtract clamps to zeroThroughput when subtrahend is larger or equal") {
    val twoPerSecond = 2.per(1.second)
    val onePerSecond = 1.per(1.second)
    assert((twoPerSecond subtract onePerSecond) === onePerSecond)
    assert((onePerSecond subtract twoPerSecond) === Rate.Zero)
    assert((onePerSecond subtract onePerSecond) === Rate.Zero)
  }

  test("subtract works across periods; subtracting zeroThroughput is identity") {
    val onePerSecond = 1.per(1.second)
    val onePerTwoSec = 1.per(2.seconds)
    assert((onePerSecond subtract onePerTwoSec) === onePerTwoSec)
    assert((onePerSecond subtract Rate.Zero) === onePerSecond)
  }

  test("validate accepts positive emission interval") {
    assertEquals(Rate(1, 1.second).validate, Right(1.second.toNanos))
    assertEquals(Rate(2, 1.second).validate, Right(500_000_000L))
  }

  test("validate rejects non-positive requests") {
    val zeroRequests     = Rate(requests = 0, period = 1.second).validate
    val negativeRequests = Rate(requests = -1, period = 1.second).validate

    assert(zeroRequests.isLeft)
    assert(zeroRequests.left.exists(_.isInstanceOf[IllegalArgumentException]))
    assert(zeroRequests.left.exists(_.getMessage.contains("rate.requests must be positive")))
    assert(negativeRequests.isLeft)
    assert(negativeRequests.left.exists(_.getMessage.contains("rate.requests must be positive")))
  }

  test("validate rejects non-positive periods") {
    val zeroPeriod     = Rate(requests = 1, period = 0.seconds).validate
    val negativePeriod = Rate(requests = 1, period = (-1).second).validate

    assert(zeroPeriod.isLeft)
    assert(zeroPeriod.left.exists(_.getMessage.contains("rate.period must be positive")))
    assert(negativePeriod.isLeft)
    assert(negativePeriod.left.exists(_.getMessage.contains("rate.period must be positive")))
  }

  test("validate rejects zero emission interval") {
    val result = Rate(requests = 2, period = 1.nanosecond).validate

    assert(result.isLeft)
    assert(result.left.exists(_.getMessage.contains("emission interval must be positive")))
  }

  test("scaleBy: 1 unchanged; below 1 slows; above 1 speeds; zero and invalid factors") {
    val fivePerSecond = 5.per(1.second) // 1 / 200 ms
    assert(fivePerSecond.scaleBy(factor = 1.0) === fivePerSecond)
    assert(fivePerSecond.scaleBy(factor = 0.5) === Rate(1, 400.millis))
    assert(fivePerSecond.scaleBy(factor = 2.0) === 10.per(1.second))
    assert(fivePerSecond.scaleBy(factor = 0.0) === 0.per(1.second))

    assert(10.per(1.second).scaleBy(factor = 0.5) === 5.per(1.second))
    assertEquals(10.per(1.second).scaleBy(factor = 0.5), 5.per(1.second))
    assert(1.per(1.second).scaleBy(factor = 0.2) === Rate(1, 5.seconds))
    assertEquals(1.per(30.seconds).scaleBy(factor = 0.5), 1.per(1.minute))

    assert(1.per(1.second).scaleBy(factor = 0.0) === Rate.Zero)
    assert(Rate.Zero.scaleBy(factor = 3.0) === Rate.Zero)
    assert(Rate.Zero.scaleBy(factor = 0.0) === Rate.Zero)

    assert(1.per(1.nanosecond).scaleBy(factor = 10.0) === 10.per(1.nanosecond))
    assert(1.per(3.nanoseconds).scaleBy(factor = 2.0) === 2.per(3.nanoseconds))
    assertEquals(1.per(3.nanoseconds).scaleBy(factor = 2.0), 2.per(3.nanoseconds))
    assertEquals(Int.MaxValue.per(1.second).scaleBy(factor = 2.0), Int.MaxValue.per(500.millis))

    intercept[IllegalArgumentException](1.per(1.second).scaleBy(factor = -0.1))
    intercept[IllegalArgumentException](1.per(1.second).scaleBy(factor = Double.NaN))
  }
}

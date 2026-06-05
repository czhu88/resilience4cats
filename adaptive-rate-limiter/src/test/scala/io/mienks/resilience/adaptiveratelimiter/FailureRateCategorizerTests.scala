package io.mienks.resilience.adaptiveratelimiter

import cats.data.NonEmptyList
import cats.effect.IO
import cats.syntax.all._
import io.mienks.resilience.adaptiveratelimiter.AdaptiveRateLimiter.FailureState._
import io.mienks.resilience.adaptiveratelimiter.AdaptiveRateLimiter.{
  FailureRateCategorizer,
  FailureState,
  HysteresisBand
}
import munit.CatsEffectSuite

class FailureRateCategorizerTests extends CatsEffectSuite {

  /** Runs a timeseries of sampled failure rates and returns the throttle signals. */
  private def runSingleBand(failureRates: Double*): IO[List[FailureState]] =
    runWithBands(
      bands = NonEmptyList.one(HysteresisBand(exit = 0.2, start = 0.5)),
      failureRates = failureRates
    )

  private def runWithBands(bands: NonEmptyList[HysteresisBand], failureRates: Seq[Double]): IO[List[FailureState]] =
    FailureRateCategorizer[IO](config = FailureRateCategorizer.Config(failureLevels = bands)).flatMap { categorizer =>
      fs2.Stream
        .emits(failureRates.toList)
        .covary[IO]
        .through(categorizer)
        .compile
        .toList
    }

  test("rejects invalid hysteresis bands") {
    List(
      FailureRateCategorizer.Config(
        failureLevels = NonEmptyList.one(HysteresisBand(exit = 0.5, start = 0.1))
      ) -> "band[0]: start >= exit to contain hysteresis",
      FailureRateCategorizer.Config(
        failureLevels = NonEmptyList.of(
          HysteresisBand(exit = 0.1, start = 0.5),
          HysteresisBand(exit = 0.2, start = 0.4)
        )
      ) -> "bands must have monotonically increasing start",
      FailureRateCategorizer.Config(
        failureLevels = NonEmptyList.of(
          HysteresisBand(exit = 0.2, start = 0.3),
          HysteresisBand(exit = 0.1, start = 0.6)
        )
      ) -> "bands must have monotonically increasing exit",
      FailureRateCategorizer.Config(
        failureLevels = NonEmptyList.of(
          HysteresisBand(exit = 0.1, start = 0.5),
          HysteresisBand(exit = 0.4, start = 0.8)
        )
      ) -> "bands must not overlap",
      FailureRateCategorizer.Config(
        failureLevels = NonEmptyList.of(
          HysteresisBand(exit = 0.1, start = 0.5),
          HysteresisBand(exit = 0.5, start = 0.8)
        )
      ) -> "bands must not overlap"
    ).traverse_ { case (config, expectedMessage) =>
      FailureRateCategorizer[IO](config = config).attempt.map { result =>
        assert(result.isLeft)
        val msg = result.left.toOption.fold("")(_.getMessage)
        assert(msg.contains(expectedMessage), clue = msg)
      }
    }
  }

  test("single band: stays at zero -> no signals") {
    runSingleBand(0.0, 0.0, 0.0).map(assertEquals(_, List.empty[FailureState]))
  }

  test("single band: hovers below start -> no signals") {
    runSingleBand(0.0, 0.2, 0.4, 0.2, 0.0).map(assertEquals(_, List.empty[FailureState]))
  }

  test("single band: above start and stays there -> Failing(0)") {
    runSingleBand(0.0, 0.6, 1.0, 1.0).map(assertEquals(_, List(Failing(level = 0))))
  }

  test("single band: above start then back into hysteresis band (no exit) -> Failing(0)") {
    runSingleBand(0.0, 0.6, 0.4).map(assertEquals(_, List(Failing(level = 0))))
  }

  test("single band: above start and drifts higher to 100% -> Failing(0)") {
    runSingleBand(0.0, 0.6, 0.8, 1.0).map(assertEquals(_, List(Failing(level = 0))))
  }

  test("single band: above start, drifts higher, then back into hysteresis band -> Failing(0)") {
    runSingleBand(0.0, 0.6, 1.0, 0.4).map(assertEquals(_, List(Failing(level = 0))))
  }

  test("single band: above start, then below exit -> Failing(0), Healthy") {
    runSingleBand(0.0, 1.0, 0.0).map(assertEquals(_, List(Failing(level = 0), Healthy)))
  }

  test("single band: above start, below exit, then back into hysteresis band (no re-enter) -> Failing(0), Healthy") {
    runSingleBand(0.0, 1.0, 0.0, 0.4).map(assertEquals(_, List(Failing(level = 0), Healthy)))
  }

  test("single band: above start, below exit, then above start again -> Failing(0), Healthy, Failing(0)") {
    runSingleBand(0.0, 1.0, 0.0, 0.6).map(assertEquals(_, List(Failing(level = 0), Healthy, Failing(level = 0))))
  }

  /*
  Three-band detector. Band thresholds are chosen with a clear gap between each band's `start` and the next band's
  `exit`, so each sample maps to at most one transition. Recovery only emits a signal on the final demotion to `Healthy`.
   */
  private val MultipleBands: NonEmptyList[HysteresisBand] = NonEmptyList.of(
    HysteresisBand(exit = 0.1, start = 0.3),
    HysteresisBand(exit = 0.4, start = 0.6),
    HysteresisBand(exit = 0.7, start = 0.9)
  )

  private def runMultipleBands(failureRates: Double*): IO[List[FailureState]] =
    runWithBands(bands = MultipleBands, failureRates = failureRates)

  test("multiple bands: stays at zero -> no signals") {
    runMultipleBands(0.0, 0.0).map(assertEquals(_, List.empty[FailureState]))
  }

  test("multiple bands: hovers below band 0 start -> no signals") {
    runMultipleBands(0.0, 0.1, 0.2, 0.1, 0.0).map(assertEquals(_, List.empty[FailureState]))
  }

  test("multiple bands: Healthy -> Failing(0) only") {
    runMultipleBands(0.0, 0.3).map(assertEquals(_, List(Failing(level = 0))))
  }

  test("multiple bands: Healthy -> Failing(0), Failing(1)") {
    runMultipleBands(0.0, 0.6).map(assertEquals(_, List(Failing(level = 0), Failing(level = 1))))
  }

  test("multiple bands: Healthy -> Failing(0), Failing(1), Failing(2)") {
    runMultipleBands(0.0, 0.9)
      .map(assertEquals(_, List(Failing(level = 0), Failing(level = 1), Failing(level = 2))))
  }

  test("multiple bands: Failing(0) -> Failing(1) retransition") {
    runMultipleBands(0.0, 0.3, 0.6).map(assertEquals(_, List(Failing(level = 0), Failing(level = 1))))
  }

  test("multiple bands: Failing(1) -> Failing(2) retransition") {
    runMultipleBands(0.0, 0.6, 0.9)
      .map(assertEquals(_, List(Failing(level = 0), Failing(level = 1), Failing(level = 2))))
  }

  test("multiple bands: climbs band by band") {
    runMultipleBands(0.0, 0.3, 0.6, 0.9)
      .map(assertEquals(_, List(Failing(level = 0), Failing(level = 1), Failing(level = 2))))
  }

  test("multiple bands: drifts higher within top band -> Failing(2)") {
    runMultipleBands(0.0, 0.9, 1.0)
      .map(assertEquals(_, List(Failing(level = 0), Failing(level = 1), Failing(level = 2))))
  }

  test("multiple bands: worsens then partially recovers (no Healthy emission) -> Failing(2)") {
    runMultipleBands(0.0, 0.9, 0.5, 0.2)
      .map(assertEquals(_, List(Failing(level = 0), Failing(level = 1), Failing(level = 2))))
  }

  test("multiple bands: full recovery from top band -> Failing(2), Healthy") {
    runMultipleBands(0.0, 0.9, 0.5, 0.2, 0.0)
      .map(
        assertEquals(_, List(Failing(level = 0), Failing(level = 1), Failing(level = 2), Healthy))
      )
  }

  test("multiple bands: re-enters top band after full recovery") {
    runMultipleBands(0.0, 0.9, 0.5, 0.2, 0.0, 0.9)
      .map(
        assertEquals(
          _,
          List(
            Failing(level = 0),
            Failing(level = 1),
            Failing(level = 2),
            Healthy,
            Failing(level = 0),
            Failing(level = 1),
            Failing(level = 2)
          )
        )
      )
  }

  test("multiple bands: re-enters Failing(1) and Failing(2) after partial recovery") {
    runMultipleBands(0.0, 0.3, 0.6, 0.9, 0.6, 0.3, 0.6, 0.9, 0.6, 0.3)
      .map(
        assertEquals(
          _,
          List(
            Failing(level = 0),
            Failing(level = 1),
            Failing(level = 2),
            Failing(level = 1),
            Failing(level = 2)
          )
        )
      )
  }
}

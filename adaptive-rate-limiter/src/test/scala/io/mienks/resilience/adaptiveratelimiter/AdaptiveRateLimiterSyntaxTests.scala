package io.mienks.resilience.adaptiveratelimiter

import cats.effect.{IO, Ref}
import cats.syntax.all._
import io.mienks.resilience.adaptiveratelimiter.AdaptiveRateLimiter.syntax._
import munit.CatsEffectSuite

/** Unit tests for the `protect` / `protectF` syntax in [[AdaptiveRateLimiter.syntax]].
  *
  * These exercise pure wiring (consume gate + outcome recording), so they use the `recording` test double with a
  * controllable `consume` rather than the live background machinery.
  */
class AdaptiveRateLimiterSyntaxTests extends CatsEffectSuite {

  test("protect: (no error, consumed allowed) -> records success") {
    for {
      rl  <- AdaptiveRateLimiter.recording[IO](canConsume = true.pure[IO])
      out <- rl.protect(fa = IO("ok"), isError = (_: String) => false, orElse = "fallback")
      _   <- IO(assertEquals(out, "ok"))
      _   <- rl.successes.get.map(assertEquals(_, 1))
      _   <- rl.failures.get.map(assertEquals(_, 0))
    } yield ()
  }

  test("protect: (error, consumed allowed) -> records failure") {
    for {
      rl  <- AdaptiveRateLimiter.recording[IO](canConsume = true.pure[IO])
      out <- rl.protect(fa = IO("boom"), isError = (_: String) => true, orElse = "fallback")
      _   <- IO(assertEquals(out, "boom"))
      _   <- rl.successes.get.map(assertEquals(_, 0))
      _   <- rl.failures.get.map(assertEquals(_, 1))
    } yield ()
  }

  test("protect: (*, consumed denied) -> orElse") {
    for {
      rl  <- AdaptiveRateLimiter.recording[IO](canConsume = false.pure[IO])
      ran <- Ref[IO].of(false)
      out <- rl.protect(fa = ran.set(true).as("ok"), isError = (_: String) => false, orElse = "fallback")
      _   <- IO(assertEquals(out, "fallback"))
      _   <- ran.get.map(b => assert(!b, clue = "effect must not run when throttled"))
      _   <- rl.successes.get.map(assertEquals(_, 0))
      _   <- rl.failures.get.map(assertEquals(_, 0))
    } yield ()
  }

  test("protectF: (no error, consumed allowed) -> records success") {
    for {
      rl  <- AdaptiveRateLimiter.recording[IO](canConsume = true.pure[IO])
      out <- rl.protectF(fa = IO("ok"), isError = (_: String) => false.pure[IO], orElse = "fallback".pure[IO])
      _   <- IO(assertEquals(out, "ok"))
      _   <- rl.successes.get.map(assertEquals(_, 1))
      _   <- rl.failures.get.map(assertEquals(_, 0))
    } yield ()
  }

  test("protectF: (error, consumed allowed) -> records failure") {
    for {
      rl  <- AdaptiveRateLimiter.recording[IO](canConsume = true.pure[IO])
      out <- rl.protectF(fa = IO("boom"), isError = (_: String) => true.pure[IO], orElse = "fallback".pure[IO])
      _   <- IO(assertEquals(out, "boom"))
      _   <- rl.successes.get.map(assertEquals(_, 0))
      _   <- rl.failures.get.map(assertEquals(_, 1))
    } yield ()
  }

  test("protectF: (*, consumed denied) -> orElse") {
    for {
      rl    <- AdaptiveRateLimiter.recording[IO](canConsume = false.pure[IO])
      ran   <- Ref[IO].of(false)
      orRan <- Ref[IO].of(false)
      out   <- rl.protectF(
        fa = ran.set(true).as("ok"),
        isError = (_: String) => false.pure[IO],
        orElse = orRan.set(true).as("fallback")
      )
      _ <- IO(assertEquals(out, "fallback"))
      _ <- ran.get.map(b => assert(!b, clue = "effect must not run when throttled"))
      _ <- orRan.get.map(b => assert(b, clue = "orElse effect should run when throttled"))
      _ <- rl.successes.get.map(assertEquals(_, 0))
      _ <- rl.failures.get.map(assertEquals(_, 0))
    } yield ()
  }
}

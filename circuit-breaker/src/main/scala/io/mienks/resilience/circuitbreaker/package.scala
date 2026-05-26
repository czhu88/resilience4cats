package io.mienks.resilience

package object circuitbreaker {
  type Measurements[F[_]] = io.mienks.resilience.Measurements[F]
  val Measurements: io.mienks.resilience.Measurements.type = io.mienks.resilience.Measurements

  type CountBasedSlidingWindowMeasurements[F[_]] = io.mienks.resilience.CountBasedSlidingWindowMeasurements[F]
  val CountBasedSlidingWindowMeasurements: io.mienks.resilience.CountBasedSlidingWindowMeasurements.type =
    io.mienks.resilience.CountBasedSlidingWindowMeasurements

  type TimeBasedSlidingWindowMeasurements[F[_]] = io.mienks.resilience.TimeBasedSlidingWindowMeasurements[F]
  val TimeBasedSlidingWindowMeasurements: io.mienks.resilience.TimeBasedSlidingWindowMeasurements.type =
    io.mienks.resilience.TimeBasedSlidingWindowMeasurements
}

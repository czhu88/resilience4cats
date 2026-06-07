package io.mienks.resilience.charts

import cats.effect.{IO, IOApp}
import cats.syntax.all._

import java.nio.file.{Files, Path, Paths}

/** Drives every [[Scenario]] through the adaptive rate limiter and writes the resulting CSV timeseries + manifest.
  *
  * Usage: `sbt "charts/run"` (optionally `sbt "charts/run <data-dir>"`). Render the charts afterwards with
  * `python charts/scripts/render.py`.
  */
object GenerateCharts extends IOApp.Simple {

  private val DefaultDataDir: Path = Paths.get("docs", "charts", "data")

  override def run: IO[Unit] =
    runWith(DefaultDataDir)

  // IOApp.Simple has no args; expose an arg-aware entry point for callers that need a custom dir.
  def runWith(dataDir: Path): IO[Unit] =
    for {
      _       <- IO.blocking { Files.createDirectories(dataDir); () }
      _       <- IO.println(s"Writing scenario data to ${dataDir.toAbsolutePath.toString}")
      entries <- Scenario.library.traverse { scenario =>
        for {
          _      <- IO.println(s"  running '${scenario.name}' ...")
          result <- SimulationRunner.run(scenario)
          entry  <- CsvWriter.writeScenario(result, dataDir)
          _      <- IO.println(
            s"    wrote ${entry.samplesFile} (${result.samples.size.toString} samples, " +
              s"${result.gradientEvents.size.toString} gradient events)"
          )
        } yield entry
      }
      manifest <- CsvWriter.writeManifest(entries, dataDir)
      _        <- IO.println(s"Wrote manifest ${manifest.toAbsolutePath.toString}")
      _        <- IO.println("Next: python charts/scripts/render.py")
    } yield ()
}

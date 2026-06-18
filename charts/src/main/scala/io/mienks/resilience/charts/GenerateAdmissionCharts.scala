package io.mienks.resilience.charts

import cats.effect.{IO, IOApp}
import cats.syntax.all._
import io.mienks.resilience.charts.admission.{AdmissionCsvWriter, AdmissionScenario, AdmissionSimulation}

import java.nio.file.{Files, Path, Paths}

/** Drives every [[AdmissionScenario]] through the admission controller and writes the resulting CSV timeseries +
  * manifest. Runs side by side with [[GenerateCharts]] (the AdaptiveRateLimiter pipeline) and writes to a separate data
  * directory so neither overwrites the other.
  *
  * Usage: `sbt "charts/runMain io.mienks.resilience.charts.GenerateAdmissionCharts"`. Render the charts afterwards with
  * `python charts/scripts/render_admission.py`.
  */
object GenerateAdmissionCharts extends IOApp.Simple {

  private val DefaultDataDir: Path = Paths.get("docs", "charts", "admission-data")

  override def run: IO[Unit] =
    runWith(DefaultDataDir)

  def runWith(dataDir: Path): IO[Unit] =
    for {
      _       <- IO.blocking { Files.createDirectories(dataDir); () }
      _       <- IO.println(s"Writing scenario data to ${dataDir.toAbsolutePath.toString}")
      entries <- AdmissionScenario.all.parTraverse { scenario =>
        for {
          _      <- IO.println(s"  running '${scenario.name}' ...")
          result <- AdmissionSimulation.run(scenario)
          entry  <- AdmissionCsvWriter.writeScenario(result, dataDir)
          _      <- IO.println(s"    wrote ${entry.samplesFile} (${result.samples.size.toString} samples)")
        } yield entry
      }
      manifest <- AdmissionCsvWriter.writeManifest(entries, dataDir)
      _        <- IO.println(s"Wrote manifest ${manifest.toAbsolutePath.toString}")
      _        <- IO.println("Next: python charts/scripts/render_admission.py")
    } yield ()
}

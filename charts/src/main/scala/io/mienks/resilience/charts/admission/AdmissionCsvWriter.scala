package io.mienks.resilience.charts.admission

import cats.effect.{IO, Sync}
import cats.syntax.all._

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

/** One row of the manifest the Python renderer iterates over. */
final case class AdmissionManifestEntry(
    scenario: String,
    description: String,
    samplesFile: String
)

/** Writes the timeseries of an AdmissionController run as plain CSV plus a manifest, so the matplotlib renderer needs
  * no Scala knowledge. There are no discrete control-loop events to export, so only a samples file is written.
  */
object AdmissionCsvWriter {

  private val SamplesHeader: String =
    "elapsed_ms,offered_rps,admitted_rps,accepted_rps,capacity_rps,rejection_probability,failure_ratio"
  private val ManifestHeader: String = "scenario,description,samples_file"

  /** Write `<scenario>-samples.csv` into `dataDir`, returning the manifest entry. */
  def writeScenario(result: AdmissionSimulation.Result, dataDir: Path): IO[AdmissionManifestEntry] = {
    val samplesFile = s"${result.scenario.name}-samples.csv"

    val samplesContent =
      (SamplesHeader +: result.samples.map { sample =>
        s"${sample.elapsedMillis.toString}," +
          s"${sample.offeredRps.toString}," +
          s"${sample.admittedRps.toString}," +
          s"${sample.acceptedRps.toString}," +
          s"${sample.capacityRps.toString}," +
          s"${sample.rejectionProbability.toString}," +
          sample.failureRatio.toString
      }).mkString("\n")

    writeFile(dataDir.resolve(samplesFile), samplesContent).as(
      AdmissionManifestEntry(
        scenario = result.scenario.name,
        description = result.scenario.description,
        samplesFile = samplesFile
      )
    )
  }

  /** Write `manifest.csv` listing every scenario; returns its path. */
  def writeManifest(entries: List[AdmissionManifestEntry], dataDir: Path): IO[Path] = {
    val manifest = dataDir.resolve("manifest.csv")
    val content  =
      (ManifestHeader +: entries.map { entry =>
        s"${escape(entry.scenario)},${escape(entry.description)},${escape(entry.samplesFile)}"
      }).mkString("\n")
    writeFile(manifest, content).as(manifest)
  }

  private def writeFile(path: Path, content: String): IO[Unit] =
    Sync[IO].blocking {
      Files.write(path, content.getBytes(StandardCharsets.UTF_8))
      ()
    }

  /** Minimal RFC-4180 escaping: quote fields containing a comma, quote, or newline. */
  private def escape(field: String): String =
    if (field.exists(c => c == ',' || c == '"' || c == '\n' || c == '\r'))
      "\"" + field.replace("\"", "\"\"") + "\""
    else field
}

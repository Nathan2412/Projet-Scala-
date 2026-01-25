package countriesEtl
import io.circe._, io.circe.generic.auto._, io.circe.syntax._
import java.nio.file.{Files, Paths}
import java.nio.charset.StandardCharsets
import java.io.{PrintWriter, File}
import scala.util.{Try, Success, Failure}


def generateReport(countries: List[Country]): AnalysisReport = {
    AnalysisReport(
        statistics = calculateStats(countries),
        top10ByPopulation = topByPopulation(countries, 10),
        top10ByArea = topByArea(countries, 10),
        top10ByGdp = topByGdp(countries, 10),
        byContinent = countByContinent(countries),
        avgPopulationByContinent = avgPopulationByContinent(countries),
        avgGdpByContinent = avgGdpByContinent(countries),
        mostSpokenLanguages = mostSpokenLanguages(countries, 10),
        currencyUsage = currencyUsage(countries, 10)
    )
}

def writeJsonReport(report: AnalysisReport, filename: String): Either[String, Unit] = {
  Try {
    // Créer le dossier parent si nécessaire
    val file = new File(filename)
    file.getParentFile.mkdirs()
   
    // Convertir en JSON avec indentation
    val jsonContent = report.asJson.spaces2
   
    // Écrire le fichier
    Files.write(Paths.get(filename), jsonContent.getBytes(StandardCharsets.UTF_8))
  } match {
    case Success(_) => Right(())
    case Failure(e) => Left(s"Erreur écriture JSON: ${e.getMessage}")
  }
}
 
/**
 * Écrit le rapport en texte lisible
 */
def writeTextReport(report: AnalysisReport, filename: String): Either[String, Unit] = {
  Try {
    val file = new File(filename)
    file.getParentFile.mkdirs()
   
    val content = generateTextContent(report)
   
    val writer = new PrintWriter(filename)
    try {
      writer.write(content)
    } finally {
      writer.close()
    }
  } match {
    case Success(_) => Right(())
    case Failure(e) => Left(s"Erreur écriture rapport: ${e.getMessage}")
  }
}
 
/**
 * Génère le contenu texte du rapport
 */
def generateTextContent(report: AnalysisReport): String = {
  val stats = report.statistics
 
  s"""===============================================
     |    RAPPORT D'ANALYSE - PAYS DU MONDE
     |===============================================
     |
     |📊 STATISTIQUES GÉNÉRALES
     |---------------------------
     |Total pays analysés    : ${stats.totalCountries}
     |Population mondiale    : ${formatNumber(stats.totalPopulation)} habitants
     |Superficie totale      : ${formatNumber(stats.totalArea.toLong)} km²
     |Population moyenne     : ${formatNumber(stats.averagePopulation.toLong)} hab/pays
     |PIB moyen              : ${f"${stats.averageGdp}%.2f"} Md$$
     |
     |🌍 TOP 10 - POPULATION
     |----------------------
     |${report.top10ByPopulation.zipWithIndex.map { case (c, i) =>
       s"${i + 1}. ${c.name.padTo(25, ' ')} : ${formatNumber(c.value)} habitants (${c.continent})"
     }.mkString("\n")}
     |
     |🗺️  TOP 10 - SUPERFICIE
     |----------------------
     |${report.top10ByArea.zipWithIndex.map { case (c, i) =>
       s"${i + 1}. ${c.name.padTo(25, ' ')} : ${formatNumber(c.value)} km² (${c.continent})"
     }.mkString("\n")}
     |
     |💰 TOP 10 - PIB
     |--------------
     |${report.top10ByGdp.zipWithIndex.map { case (c, i) =>
       s"${i + 1}. ${c.name.padTo(25, ' ')} : ${c.gdp.map(g => f"$g%.0f").getOrElse("N/A")} Md$$ (${c.continent})"
     }.mkString("\n")}
     |
     |🌎 PAR CONTINENT
     |----------------
     |${report.byContinent.toSeq.sortBy(-_._2).map { case (cont, count) =>
       s"${cont.padTo(15, ' ')} : $count pays"
     }.mkString("\n")}
     |
     |👥 POPULATION MOYENNE PAR CONTINENT
     |----------------------------------
     |${report.avgPopulationByContinent.toSeq.sortBy(-_._2).map { case (cont, avg) =>
       s"${cont.padTo(15, ' ')} : ${formatNumber(avg)} habitants"
     }.mkString("\n")}
     |
     |💵 PIB MOYEN PAR CONTINENT
     |-------------------------
     |${report.avgGdpByContinent.toSeq.sortBy(-_._2).map { case (cont, avg) =>
       s"${cont.padTo(15, ' ')} : $avg Md$$"
     }.mkString("\n")}
     |
     |🗣️  LANGUES LES PLUS PARLÉES
     |---------------------------
     |${report.mostSpokenLanguages.toSeq.sortBy(-_._2).map { case (lang, count) =>
       s"${lang.padTo(15, ' ')} : $count pays"
     }.mkString("\n")}
     |
     |💱 DEVISES LES PLUS UTILISÉES
     |-----------------------------
     |${report.currencyUsage.toSeq.sortBy(-_._2).map { case (curr, count) =>
       s"${curr.padTo(15, ' ')} : $count pays"
     }.mkString("\n")}
     |
     |===============================================
     |         Rapport généré avec Scala 3
     |===============================================
     |""".stripMargin
}
 
/**
 * Formate les grands nombres avec des espaces
 */
def formatNumber(number: Long): String = {
  number.toString.reverse.grouped(3).mkString(" ").reverse
}
 
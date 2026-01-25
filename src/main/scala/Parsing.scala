package countriesEtl

import io.circe.parser.decode
import io.circe.generic.auto._
import scala.io.Source
import scala.util.{Try, Success, Failure}

def loadCountries(filename: String): Either[String, List[Country]] = {
  val contentResult = Try {
    val source = Source.fromFile(filename)
    try {
      source.mkString
    } finally {
      source.close()
    }
  }

  contentResult match {
    case Success(content) =>
      decode[List[Country]](content) match {
        case Right(countries) => Right(countries)
        case Left(error) => Left(s"Erreur parsing JSON: ${error.getMessage}")
      }
    case Failure(exception) =>
      Left(s"Erreur lecture fichier $filename: ${exception.getMessage}")
  }
}

/**
 * Charge tous les fichiers de données (clean, dirty, large)
 */
def loadAllFiles(dataDir: String): Either[String, (List[Country], Int)] = {
  val files = List(
    s"$dataDir/data_clean.json",
    s"$dataDir/data_dirty.json",
    s"$dataDir/data_large.json"
  )

  // Charger chaque fichier et accumuler
  val results = files.map(loadCountries)
  
  // Compter les erreurs et récupérer les pays valides
  val (successes, failures) = results.partition(_.isRight)
  val allCountries = successes.flatMap(_.toOption.get)
  val errorCount = failures.length

  if (allCountries.isEmpty && errorCount > 0) {
    Left("Aucun fichier n'a pu être lu")
  } else {
    Right((allCountries, errorCount))
  }
}



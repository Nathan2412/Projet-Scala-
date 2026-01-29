package countriesEtl

import scala.io.Source
import java.io.{PrintWriter, File}
import io.circe.generic.auto._

def chargerUnFichier(chemin: String): List[Country] =
  try
    val source = Source.fromFile(chemin)
    val jsonText = source.mkString
    source.close()
    io.circe.parser.decode[List[Country]](jsonText) match
      case Right(pays) => pays
      case Left(_) => List.empty
  catch
    case _: Exception => List.empty

def nettoyerDonnees(countries: List[Country]): List[Country] =
  countries.filter { pays =>
    pays.name.isDefined && pays.name.get.nonEmpty &&
    pays.population.isDefined && pays.population.get > 0 &&
    pays.continent.isDefined
  }

def extraireNomBase(nom: Option[String]): String =
  nom.getOrElse("").replaceAll("\\s*\\(\\d+\\)$", "")

def enleverDoublons(countries: List[Country]): List[Country] =
  countries
    .filter(_.name.isDefined)
    .groupBy(c => extraireNomBase(c.name))
    .map { case (nomBase, liste) => liste.head.copy(name = Some(nomBase)) }
    .toList

def extraireNomFichier(chemin: String): String =
  val fichier = chemin.split("/").last.replace(".json", "")
  fichier.replace("data_", "")

def sauvegarderRapportFichier(
    nomSource: String,
    countries: List[Country],
    entreesTotales: Int,
    objetsInvalides: Int,
    doublons: Int,
    tempsMs: Long
): Unit =
  try
    val outputDir = new File("output")
    if !outputDir.exists() then outputDir.mkdirs()

    val baseName = s"rapport-$nomSource"
    val writerTxt = new PrintWriter(s"output/$baseName.txt")

    writerTxt.println("===============================================")
    writerTxt.println(s"     RAPPORT - $nomSource")
    writerTxt.println("===============================================")
    writerTxt.println()
    writerTxt.println("STATISTIQUES DE PARSING")
    writerTxt.println("---------------------------")
    writerTxt.println(s"- Entrees totales       : $entreesTotales")
    writerTxt.println(s"- Objets invalides      : $objetsInvalides")
    writerTxt.println(s"- Doublons              : $doublons")
    writerTxt.println(s"- Nb valide             : ${countries.size}")
    writerTxt.println()

    val populationTotale = countries.flatMap(_.population).sum
    val superficieTotale = countries.flatMap(_.area).sum.toLong

    writerTxt.println("STATISTIQUES")
    writerTxt.println("---------------------------")
    writerTxt.println(s"- Pays valides      : ${countries.size}")
    writerTxt.println(s"- Population totale : $populationTotale")
    writerTxt.println(s"- Superficie totale : $superficieTotale km2")
    writerTxt.println()

    val top5Pop = topByPopulation(countries, 5)
    writerTxt.println("TOP 5 - POPULATION")
    writerTxt.println("----------------------")
    top5Pop.zipWithIndex.foreach { case (pays, i) =>
      writerTxt.println(s"${i+1}. ${pays.name} : ${pays.value} hab.")
    }
    writerTxt.println()

    val parContinent = countByContinent(countries)
    writerTxt.println("PAR CONTINENT")
    writerTxt.println("-----------------")
    parContinent.toSeq.sortBy(_._1).foreach { case (continent, nb) =>
      writerTxt.println(s"- $continent : $nb pays")
    }
    writerTxt.println()

    val tempsSec = tempsMs / 1000.0
    writerTxt.println(f"Temps : $tempsSec%.3f sec")
    writerTxt.println("===============================================")
    writerTxt.close()

    // JSON
    val writerJson = new PrintWriter(s"output/$baseName.json")
    val top5PopJson = top5Pop.map(p => s"""    {"name": "${p.name}", "population": ${p.value}}""").mkString(",\n")
    val continentJson = parContinent.toSeq.sortBy(_._1).map { case (c, n) => s"""    "$c": $n""" }.mkString(",\n")

    writerJson.println("{")
    writerJson.println(s"""  "source": "$nomSource",""")
    writerJson.println(s"""  "parsing": {""")
    writerJson.println(s"""    "entrees_totales": $entreesTotales,""")
    writerJson.println(s"""    "objets_invalides": $objetsInvalides,""")
    writerJson.println(s"""    "doublons": $doublons,""")
    writerJson.println(s"""    "nb_valide": ${countries.size}""")
    writerJson.println(s"""  },""")
    writerJson.println(s"""  "statistics": {""")
    writerJson.println(s"""    "population_totale": $populationTotale,""")
    writerJson.println(s"""    "superficie_totale": $superficieTotale""")
    writerJson.println(s"""  },""")
    writerJson.println(s"""  "top_5_population": [""")
    writerJson.println(top5PopJson)
    writerJson.println(s"""  ],""")
    writerJson.println(s"""  "par_continent": {""")
    writerJson.println(continentJson)
    writerJson.println(s"""  }""")
    writerJson.println("}")
    writerJson.close()

    println(s"  -> Rapport sauvegarde : output/$baseName.txt et .json")
  catch
    case e: Exception => println(s"Erreur sauvegarde $nomSource: ${e.getMessage}")

def sauvegarderRapportGlobal(
    countries: List[Country],
    entreesTotales: Int,
    objetsInvalides: Int,
    doublons: Int,
    tempsMs: Long
): Unit =
  try
    val outputDir = new File("output")
    if !outputDir.exists() then outputDir.mkdirs()

    val writerTxt = new PrintWriter("output/rapport.txt")

    writerTxt.println("===============================================")
    writerTxt.println("     RAPPORT GLOBAL - PAYS DU MONDE")
    writerTxt.println("===============================================")
    writerTxt.println()
    writerTxt.println("STATISTIQUES DE PARSING")
    writerTxt.println("---------------------------")
    writerTxt.println(s"- Entrees totales       : $entreesTotales")
    writerTxt.println(s"- Objets invalides      : $objetsInvalides")
    writerTxt.println(s"- Doublons              : $doublons")
    writerTxt.println(s"- Nb valide             : ${countries.size}")
    writerTxt.println()

    val populationTotale = countries.flatMap(_.population).sum
    val superficieTotale = countries.flatMap(_.area).sum.toLong

    writerTxt.println("STATISTIQUES GLOBALES")
    writerTxt.println("---------------------------")
    writerTxt.println(s"- Population totale : $populationTotale habitants")
    writerTxt.println(s"- Superficie totale : $superficieTotale km2")
    writerTxt.println()

    val top10Pop = topByPopulation(countries, 10)
    writerTxt.println("TOP 10 - POPULATION")
    writerTxt.println("----------------------")
    top10Pop.zipWithIndex.foreach { case (pays, i) =>
      writerTxt.println(s"${i+1}. ${pays.name} : ${pays.value} hab.")
    }
    writerTxt.println()

    val top10Area = topByArea(countries, 10)
    writerTxt.println("TOP 10 - SUPERFICIE")
    writerTxt.println("-----------------------")
    top10Area.zipWithIndex.foreach { case (pays, i) =>
      writerTxt.println(s"${i+1}. ${pays.name} : ${pays.value} km2")
    }
    writerTxt.println()

    val top10Gdp = topByGdp(countries, 10)
    writerTxt.println("TOP 10 - PIB")
    writerTxt.println("---------------")
    top10Gdp.zipWithIndex.foreach { case (pays, i) =>
      writerTxt.println(s"${i+1}. ${pays.name} : ${pays.value} milliards USD")
    }
    writerTxt.println()

    val parContinent = countByContinent(countries)
    writerTxt.println("PAR CONTINENT")
    writerTxt.println("-----------------")
    parContinent.toSeq.sortBy(_._1).foreach { case (continent, nb) =>
      writerTxt.println(s"- $continent : $nb pays")
    }
    writerTxt.println()

    val langues = mostSpokenLanguages(countries, 5)
    writerTxt.println("LANGUES LES PLUS REPANDUES")
    writerTxt.println("-----------------------------")
    langues.toSeq.sortBy(-_._2).zipWithIndex.foreach { case ((langue, nb), i) =>
      writerTxt.println(s"${i+1}. $langue : $nb pays")
    }
    writerTxt.println()

    val multilingues = paysMultilingues(countries)
    writerTxt.println(s"PAYS MULTILINGUES (>= 3 langues) : ${multilingues.size}")
    writerTxt.println()

    val tempsSec = tempsMs / 1000.0
    writerTxt.println(f"Temps total : $tempsSec%.3f sec")
    writerTxt.println("===============================================")
    writerTxt.close()

    // JSON global
    val writerJson = new PrintWriter("output/rapport.json")
    val top10PopJson = top10Pop.map(p => s"""    {"name": "${p.name}", "population": ${p.value}, "continent": "${p.continent}"}""").mkString(",\n")
    val top10AreaJson = top10Area.map(p => s"""    {"name": "${p.name}", "area": ${p.value}, "continent": "${p.continent}"}""").mkString(",\n")
    val top10GdpJson = top10Gdp.map(p => s"""    {"name": "${p.name}", "gdp": ${p.value}, "continent": "${p.continent}"}""").mkString(",\n")
    val continentJson = parContinent.toSeq.sortBy(_._1).map { case (c, n) => s"""    "$c": $n""" }.mkString(",\n")
    val languesJson = langues.toSeq.sortBy(-_._2).map { case (l, n) => s"""    {"language": "$l", "count": $n}""" }.mkString(",\n")

    writerJson.println("{")
    writerJson.println(s"""  "parsing": {""")
    writerJson.println(s"""    "entrees_totales": $entreesTotales,""")
    writerJson.println(s"""    "objets_invalides": $objetsInvalides,""")
    writerJson.println(s"""    "doublons": $doublons,""")
    writerJson.println(s"""    "nb_valide": ${countries.size}""")
    writerJson.println(s"""  },""")
    writerJson.println(s"""  "statistics": {""")
    writerJson.println(s"""    "population_totale": $populationTotale,""")
    writerJson.println(s"""    "superficie_totale": $superficieTotale""")
    writerJson.println(s"""  },""")
    writerJson.println(s"""  "top_10_population": [""")
    writerJson.println(top10PopJson)
    writerJson.println(s"""  ],""")
    writerJson.println(s"""  "top_10_area": [""")
    writerJson.println(top10AreaJson)
    writerJson.println(s"""  ],""")
    writerJson.println(s"""  "top_10_gdp": [""")
    writerJson.println(top10GdpJson)
    writerJson.println(s"""  ],""")
    writerJson.println(s"""  "par_continent": {""")
    writerJson.println(continentJson)
    writerJson.println(s"""  },""")
    writerJson.println(s"""  "langues_populaires": [""")
    writerJson.println(languesJson)
    writerJson.println(s"""  ]""")
    writerJson.println("}")
    writerJson.close()

    println(s"\nRapport global sauvegarde : output/rapport.txt et .json")
  catch
    case e: Exception => println(s"Erreur : ${e.getMessage}")

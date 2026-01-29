package countriesEtl

import scala.io.Source
import java.io.{PrintWriter, File}

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
  countries.filter(pays => pays.name.nonEmpty && pays.population > 0 && pays.continent.nonEmpty)

def extraireNomBase(nom: String): String =
  nom.replaceAll("\\s*\\(\\d+\\)$", "")

def enleverDoublons(countries: List[Country]): List[Country] =
  countries.filter(_.name.nonEmpty).groupBy(c => extraireNomBase(c.name)).map { case (nomBase, liste) =>
    liste.head.copy(name = nomBase)
  }.toList

def analyserUnFichier(nom: String, countries: List[Country]): Unit =
  println(s"\n--- Analyse de $nom ---")
  println(s"Pays charges : ${countries.size}")
  if countries.isEmpty then
    println("Aucune donnee a analyser")
    return
  val valides = nettoyerDonnees(countries)
  println(s"Pays valides : ${valides.size}")
  if valides.isEmpty then
    println("Aucun pays valide")
    return
  val populationTotale = valides.map(_.population).sum
  println(s"Population totale : $populationTotale")
  println("Top 3 population :")
  valides.filter(_.population > 0).sortBy(-_.population).take(3).foreach { pays =>
    println(s"  - ${pays.name} : ${pays.population}")
  }

def analyserTousLesFichiers(): (List[Country], List[Country], List[Country]) =
  val dataDir = "fp-scala-etl-project/1-countries"
  println("=== CHARGEMENT DES FICHIERS ===")
  val clean = chargerUnFichier(s"$dataDir/data_clean.json")
  val dirty = chargerUnFichier(s"$dataDir/data_dirty.json")
  val large = chargerUnFichier(s"$dataDir/data_large.json")
  (clean, dirty, large)

def agregerDonnees(clean: List[Country], dirty: List[Country], large: List[Country]): List[Country] =
  println("\n=== AGREGATION DES DONNEES ===")
  val tousLesPays = clean ++ dirty ++ large
  println(s"Total avant agregation : ${tousLesPays.size}")
  val sansDoublons = enleverDoublons(tousLesPays)
  println(s"Total apres agregation (sans doublons) : ${sansDoublons.size}")
  val valides = nettoyerDonnees(sansDoublons)
  println(s"Pays valides pour analyse : ${valides.size}")
  valides

def afficherStatistiques(countries: List[Country]): Unit =
  println("\n=== STATISTIQUES GLOBALES ===")
  val populationTotale = countries.map(_.population).sum
  val superficieTotale = countries.map(_.area).sum.toLong
  println(s"Nombre de pays : ${countries.size}")
  println(s"Population totale : $populationTotale habitants")
  println(s"Superficie totale : $superficieTotale km2")
  if countries.nonEmpty then
    val popMoyenne = populationTotale / countries.size
    println(s"Population moyenne : $popMoyenne habitants/pays")

def afficherTop5(countries: List[Country]): Unit =
  println("\n=== TOP 5 POPULATION ===")
  countries.filter(_.population > 0).sortBy(-_.population).take(5).zipWithIndex.foreach { case (pays, i) =>
    println(s"${i+1}. ${pays.name} : ${pays.population} habitants")
  }
  println("\n=== TOP 5 SUPERFICIE ===")
  countries.filter(_.area > 0).sortBy(-_.area).take(5).zipWithIndex.foreach { case (pays, i) =>
    println(s"${i+1}. ${pays.name} : ${pays.area.toLong} km2")
  }
  println("\n=== TOP 5 PIB ===")
  countries.filter(_.gdp > 0).sortBy(-_.gdp).take(5).zipWithIndex.foreach { case (pays, i) =>
    println(s"${i+1}. ${pays.name} : ${pays.gdp.toLong} milliards")
  }

def analyserParContinent(countries: List[Country]): Unit =
  println("\n=== PAR CONTINENT ===")
  val parContinent = countries.filter(_.continent.nonEmpty).groupBy(_.continent)
  parContinent.toSeq.sortBy(_._1).foreach { case (continent, pays) =>
    val pop = pays.map(_.population).sum
    println(s"$continent : ${pays.size} pays, $pop habitants")
  }

def sauvegarderRapport(countries: List[Country], entreesTotales: Int, objetsInvalides: Int, doublons: Int, tempsMs: Long): Unit =
  try
    val outputDir = new File("output")
    if !outputDir.exists() then outputDir.mkdirs()

    val txtFile = new File("output/rapport.txt")
    val jsonFile = new File("output/rapport.json")
    if txtFile.exists() then txtFile.delete()
    if jsonFile.exists() then jsonFile.delete()

    val writerTxt = new PrintWriter("output/rapport.txt")
    writerTxt.println("===============================================")
    writerTxt.println("     RAPPORT D'ANALYSE - PAYS DU MONDE")
    writerTxt.println("===============================================")
    writerTxt.println()
    writerTxt.println("STATISTIQUES DE PARSING")
    writerTxt.println("---------------------------")
    writerTxt.println(s"- Entrees totales       : $entreesTotales")
    writerTxt.println(s"- Objets invalides      : $objetsInvalides")
    writerTxt.println(s"- Doublons              : $doublons")
    writerTxt.println(s"- Nb valide             : ${countries.size}")
    writerTxt.println()

    val populationTotale = countries.map(_.population).sum
    val superficieTotale = countries.map(_.area).sum.toLong

    writerTxt.println("STATISTIQUES GLOBALES")
    writerTxt.println("---------------------------")
    writerTxt.println(s"- Population totale : $populationTotale habitants")
    writerTxt.println(s"- Superficie totale : $superficieTotale km2")
    writerTxt.println()

    val top10Pop = countries.filter(_.population > 0).sortBy(-_.population).take(10)
    writerTxt.println("TOP 10 - POPULATION")
    writerTxt.println("----------------------")
    top10Pop.zipWithIndex.foreach { case (pays, i) =>
      writerTxt.println(s"${i+1}. ${pays.name} : ${pays.population} hab.")
    }
    writerTxt.println()

    val top10Area = countries.filter(_.area > 0).sortBy(-_.area).take(10)
    writerTxt.println("TOP 10 - SUPERFICIE")
    writerTxt.println("-----------------------")
    top10Area.zipWithIndex.foreach { case (pays, i) =>
      writerTxt.println(s"${i+1}. ${pays.name} : ${pays.area.toLong} km2")
    }
    writerTxt.println()

    val top10Gdp = countries.filter(_.gdp > 0).sortBy(-_.gdp).take(10)
    writerTxt.println("TOP 10 - PIB")
    writerTxt.println("---------------")
    top10Gdp.zipWithIndex.foreach { case (pays, i) =>
      writerTxt.println(s"${i+1}. ${pays.name} : ${pays.gdp.toLong} milliards USD")
    }
    writerTxt.println()

    val parContinent = countries.filter(_.continent.nonEmpty).groupBy(_.continent).toSeq.sortBy(_._1)
    writerTxt.println("REPARTITION PAR CONTINENT")
    writerTxt.println("-----------------------------")
    parContinent.foreach { case (continent, pays) =>
      writerTxt.println(s"- $continent : ${pays.size} pays")
    }
    writerTxt.println()

    writerTxt.println("MOYENNES PAR CONTINENT")
    writerTxt.println("--------------------------")
    parContinent.foreach { case (continent, pays) =>
      val popMoy = pays.map(_.population).sum / pays.size
      writerTxt.println(s"- $continent : $popMoy hab. (moyenne)")
    }
    writerTxt.println()

    val langues = countries.flatMap(_.languages).groupBy(identity).view.mapValues(_.size).toList.sortBy(-_._2).take(5)
    writerTxt.println("LANGUES LES PLUS REPANDUES")
    writerTxt.println("--------------------------------")
    langues.zipWithIndex.foreach { case ((langue, nb), i) =>
      writerTxt.println(s"${i+1}. $langue : $nb pays")
    }
    writerTxt.println()

    val multilingues = countries.filter(_.languages.length >= 3)
    writerTxt.println(s"PAYS MULTILINGUES (>= 3 langues) : ${multilingues.size} pays")
    writerTxt.println()

    val tempsSec = tempsMs / 1000.0
    writerTxt.println("PERFORMANCE")
    writerTxt.println("---------------")
    writerTxt.println(f"- Temps de traitement : $tempsSec%.3f secondes")
    writerTxt.println()
    writerTxt.println("===============================================")
    writerTxt.close()

    val writerJson = new PrintWriter("output/rapport.json")

    val top10PopJson = top10Pop.map { pays =>
      s"""    {"name": "${pays.name}", "population": ${pays.population}, "continent": "${pays.continent}"}"""
    }.mkString(",\n")

    val top10AreaJson = top10Area.map { pays =>
      s"""    {"name": "${pays.name}", "area": ${pays.area.toLong}, "continent": "${pays.continent}"}"""
    }.mkString(",\n")

    val top10GdpJson = top10Gdp.map { pays =>
      s"""    {"name": "${pays.name}", "gdp": ${pays.gdp.toLong}, "continent": "${pays.continent}"}"""
    }.mkString(",\n")

    val continentJson = parContinent.map { case (continent, pays) =>
      s"""    "$continent": ${pays.size}"""
    }.mkString(",\n")

    val avgPopJson = parContinent.map { case (continent, pays) =>
      val avg = pays.map(_.population).sum / pays.size
      s"""    "$continent": $avg"""
    }.mkString(",\n")

    val languesJson = langues.map { case (langue, nb) =>
      s"""    {"language": "$langue", "count": $nb}"""
    }.mkString(",\n")

    val multiJson = multilingues.take(10).map { pays =>
      val langs = pays.languages.map(l => s""""$l"""").mkString(", ")
      s"""    {"name": "${pays.name}", "languages": [$langs]}"""
    }.mkString(",\n")

    writerJson.println("{")
    writerJson.println(s"""  "statistics": {""")
    writerJson.println(s"""    "entrees_totales": $entreesTotales,""")
    writerJson.println(s"""    "objets_invalides": $objetsInvalides,""")
    writerJson.println(s"""    "doublons": $doublons,""")
    writerJson.println(s"""    "nb_valide": ${countries.size}""")
    writerJson.println(s"""  },""")
    writerJson.println(s"""  "top_10_by_population": [""")
    writerJson.println(top10PopJson)
    writerJson.println(s"""  ],""")
    writerJson.println(s"""  "top_10_by_area": [""")
    writerJson.println(top10AreaJson)
    writerJson.println(s"""  ],""")
    writerJson.println(s"""  "top_10_by_gdp": [""")
    writerJson.println(top10GdpJson)
    writerJson.println(s"""  ],""")
    writerJson.println(s"""  "countries_by_continent": {""")
    writerJson.println(continentJson)
    writerJson.println(s"""  },""")
    writerJson.println(s"""  "average_population_by_continent": {""")
    writerJson.println(avgPopJson)
    writerJson.println(s"""  },""")
    writerJson.println(s"""  "most_common_languages": [""")
    writerJson.println(languesJson)
    writerJson.println(s"""  ],""")
    writerJson.println(s"""  "multilingual_countries": [""")
    writerJson.println(multiJson)
    writerJson.println(s"""  ]""")
    writerJson.println("}")
    writerJson.close()

    println(s"\nRapports sauvegardes dans output/rapport.txt et output/rapport.json")
  catch
    case e: Exception => println(s"Erreur : ${e.getMessage}")

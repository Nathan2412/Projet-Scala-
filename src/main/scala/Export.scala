package countriesEtl

import scala.io.Source
import java.io.PrintWriter
import java.io.File
import io.circe.generic.auto._

def chargerUnFichier(chemin: String): List[Country] = {
  try {
    val source = Source.fromFile(chemin)
    val jsonText = source.mkString
    source.close()
    
    io.circe.parser.decode[List[Country]](jsonText) match {
      case Right(pays) => pays
      case Left(err) => List.empty
    }
  } catch {
    case e: Exception => List.empty
  }
}

def nettoyerDonnees(countries: List[Country]): List[Country] = {
  countries.filter { pays =>
    pays.name.isDefined && pays.name.get.nonEmpty && pays.population.isDefined && pays.population.get > 0 && pays.continent.isDefined
  }
}

def extraireNomBase(nom: Option[String]): String = {
  nom.getOrElse("").replaceAll("\\s*\\(\\d+\\)$", "")
}

def enleverDoublons(countries: List[Country]): List[Country] = {
  countries.filter(_.name.isDefined).groupBy(c => extraireNomBase(c.name)).map { case (nomBase, liste) => 
    liste.head.copy(name = Some(nomBase))
  }.toList
}

def analyserUnFichier(nom: String, countries: List[Country]): Unit = {
  println(s"\n--- Analyse de $nom ---")
  println(s"Pays charges : ${countries.size}")
  
  if (countries.isEmpty) {
    println("Aucune donnee a analyser")
    return
  }
  
  val valides = nettoyerDonnees(countries)
  println(s"Pays valides : ${valides.size}")
  
  if (valides.isEmpty) {
    println("Aucun pays valide")
    return
  }
  
  val populationTotale = valides.flatMap(_.population).sum
  println(s"Population totale : $populationTotale")
  
  println("Top 3 population :")
  valides.filter(_.population.isDefined).sortBy(_.population.get).reverse.take(3).foreach { pays =>
    println(s"  - ${pays.name.getOrElse("?")} : ${pays.population.get}")
  }
}

def analyserTousLesFichiers(): (List[Country], List[Country], List[Country]) = {
  val dataDir = "fp-scala-etl-project/1-countries"
  
  println("=== CHARGEMENT DES FICHIERS ===")
  
  val clean = chargerUnFichier(s"$dataDir/data_clean.json")
  val dirty = chargerUnFichier(s"$dataDir/data_dirty.json")
  val large = chargerUnFichier(s"$dataDir/data_large.json")
  
  (clean, dirty, large)
}

def agregerDonnees(clean: List[Country], dirty: List[Country], large: List[Country]): List[Country] = {
  println("\n=== AGREGATION DES DONNEES ===")
  
  val tousLesPays = clean ++ dirty ++ large
  println(s"Total avant agregation : ${tousLesPays.size}")
  
  val sansDoublons = enleverDoublons(tousLesPays)
  println(s"Total apres agregation (sans doublons) : ${sansDoublons.size}")
  
  val valides = nettoyerDonnees(sansDoublons)
  println(s"Pays valides pour analyse : ${valides.size}")
  
  valides
}

def afficherStatistiques(countries: List[Country]): Unit = {
  println("\n=== STATISTIQUES GLOBALES ===")
  
  val populationTotale = countries.flatMap(_.population).sum
  val superficieTotale = countries.flatMap(_.area).sum.toLong
  
  println(s"Nombre de pays : ${countries.size}")
  println(s"Population totale : $populationTotale habitants")
  println(s"Superficie totale : $superficieTotale km2")
  
  if (countries.nonEmpty) {
    val popMoyenne = populationTotale / countries.size
    println(s"Population moyenne : $popMoyenne habitants/pays")
  }
}

def afficherTop5(countries: List[Country]): Unit = {
  println("\n=== TOP 5 POPULATION ===")
  countries.filter(_.population.isDefined).sortBy(_.population.get).reverse.take(5).zipWithIndex.foreach { case (pays, i) =>
    println(s"${i+1}. ${pays.name.getOrElse("?")} : ${pays.population.get} habitants")
  }
  
  println("\n=== TOP 5 SUPERFICIE ===")
  countries.filter(_.area.isDefined).sortBy(_.area.get).reverse.take(5).zipWithIndex.foreach { case (pays, i) =>
    println(s"${i+1}. ${pays.name.getOrElse("?")} : ${pays.area.get.toLong} km2")
  }
  
  println("\n=== TOP 5 PIB ===")
  countries.filter(_.gdp.isDefined).sortBy(_.gdp.get).reverse.take(5).zipWithIndex.foreach { case (pays, i) =>
    println(s"${i+1}. ${pays.name.getOrElse("?")} : ${pays.gdp.get.toLong} milliards")
  }
}

def analyserParContinent(countries: List[Country]): Unit = {
  println("\n=== PAR CONTINENT ===")
  
  val parContinent = countries.filter(_.continent.isDefined).groupBy(_.continent.get)
  parContinent.toSeq.sortBy(_._1).foreach { case (continent, pays) =>
    val pop = pays.flatMap(_.population).sum
    println(s"$continent : ${pays.size} pays, $pop habitants")
  }
}

def sauvegarderRapport(countries: List[Country], nbClean: Int, nbDirty: Int, nbLarge: Int, doublonsSup: Int, tempsMs: Long): Unit = {
  try {
    val outputDir = new File("output")
    if (!outputDir.exists()) outputDir.mkdirs()
    
    val txtFile = new File("output/rapport.txt")
    val jsonFile = new File("output/rapport.json")
    if (txtFile.exists()) txtFile.delete()
    if (jsonFile.exists()) jsonFile.delete()
    
    val writerTxt = new PrintWriter("output/rapport.txt")
    
    writerTxt.println("===============================================")
    writerTxt.println("     RAPPORT D'ANALYSE - PAYS DU MONDE")
    writerTxt.println("===============================================")
    writerTxt.println()
    writerTxt.println("STATISTIQUES DE PARSING")
    writerTxt.println("---------------------------")
    writerTxt.println(s"- Entrees totales lues      : ${nbClean + nbDirty + nbLarge}")
    writerTxt.println(s"- Entrees valides           : ${countries.size}")
    writerTxt.println(s"- Doublons supprimes        : $doublonsSup")
    writerTxt.println()
    
    val populationTotale = countries.flatMap(_.population).sum
    val superficieTotale = countries.flatMap(_.area).sum.toLong
    
    writerTxt.println("STATISTIQUES GLOBALES")
    writerTxt.println("---------------------------")
    writerTxt.println(s"- Population totale : $populationTotale habitants")
    writerTxt.println(s"- Superficie totale : $superficieTotale km2")
    writerTxt.println()
    
    // Top 10 population
    val top10Pop = countries.filter(_.population.isDefined).sortBy(_.population.get).reverse.take(10)
    writerTxt.println("TOP 10 - POPULATION")
    writerTxt.println("----------------------")
    top10Pop.zipWithIndex.foreach { case (pays, i) =>
      writerTxt.println(s"${i+1}. ${pays.name.getOrElse("?")} : ${pays.population.get} hab.")
    }
    writerTxt.println()
    
    // Top 10 superficie
    val top10Area = countries.filter(_.area.isDefined).sortBy(_.area.get).reverse.take(10)
    writerTxt.println("TOP 10 - SUPERFICIE")
    writerTxt.println("-----------------------")
    top10Area.zipWithIndex.foreach { case (pays, i) =>
      writerTxt.println(s"${i+1}. ${pays.name.getOrElse("?")} : ${pays.area.get.toLong} km2")
    }
    writerTxt.println()
    
    // Top 10 PIB
    val top10Gdp = countries.filter(_.gdp.isDefined).sortBy(_.gdp.get).reverse.take(10)
    writerTxt.println("TOP 10 - PIB")
    writerTxt.println("---------------")
    top10Gdp.zipWithIndex.foreach { case (pays, i) =>
      writerTxt.println(s"${i+1}. ${pays.name.getOrElse("?")} : ${pays.gdp.get.toLong} milliards USD")
    }
    writerTxt.println()
    
    // Par continent
    val parContinent = countries.filter(_.continent.isDefined).groupBy(_.continent.get).toSeq.sortBy(_._1)
    writerTxt.println("REPARTITION PAR CONTINENT")
    writerTxt.println("-----------------------------")
    parContinent.foreach { case (continent, pays) =>
      writerTxt.println(s"- $continent : ${pays.size} pays")
    }
    writerTxt.println()
    
    // Population moyenne par continent
    writerTxt.println("MOYENNES PAR CONTINENT")
    writerTxt.println("--------------------------")
    parContinent.foreach { case (continent, pays) =>
      val popMoy = pays.flatMap(_.population).sum / pays.size
      writerTxt.println(s"- $continent : $popMoy hab. (moyenne)")
    }
    writerTxt.println()
    
    // Langues les plus parlees
    val langues = countries.flatMap(_.languages).groupBy(identity).view.mapValues(_.size).toList.sortBy(-_._2).take(5)
    writerTxt.println("LANGUES LES PLUS REPANDUES")
    writerTxt.println("--------------------------------")
    langues.zipWithIndex.foreach { case ((langue, nb), i) =>
      writerTxt.println(s"${i+1}. $langue : $nb pays")
    }
    writerTxt.println()
    
    // Pays multilingues
    val multilingues = countries.filter(_.languages.length >= 3)
    writerTxt.println(s"PAYS MULTILINGUES (>= 3 langues) : ${multilingues.size} pays")
    writerTxt.println()
    
    // Performance
    val tempsSec = tempsMs / 1000.0
    writerTxt.println("PERFORMANCE")
    writerTxt.println("---------------")
    writerTxt.println(f"- Temps de traitement : $tempsSec%.3f secondes")
    writerTxt.println()
    writerTxt.println("===============================================")
    
    writerTxt.close()
    
    val writerJson = new PrintWriter("output/rapport.json")
    
    // Top 10 population JSON
    val top10PopJson = top10Pop.map { pays =>
      s"""    {"name": "${pays.name.getOrElse("?")}", "population": ${pays.population.get}, "continent": "${pays.continent.getOrElse("?")}"}"""
    }.mkString(",\n")
    
    // Top 10 superficie JSON
    val top10AreaJson = top10Area.map { pays =>
      s"""    {"name": "${pays.name.getOrElse("?")}", "area": ${pays.area.get.toLong}, "continent": "${pays.continent.getOrElse("?")}"}"""
    }.mkString(",\n")
    
    // Top 10 PIB JSON
    val top10GdpJson = top10Gdp.map { pays =>
      s"""    {"name": "${pays.name.getOrElse("?")}", "gdp": ${pays.gdp.get.toLong}, "continent": "${pays.continent.getOrElse("?")}"}"""
    }.mkString(",\n")
    
    // Continents JSON
    val continentJson = parContinent.map { case (continent, pays) =>
      s"""    "$continent": ${pays.size}"""
    }.mkString(",\n")
    
    // Moyenne population par continent
    val avgPopJson = parContinent.map { case (continent, pays) =>
      val avg = pays.flatMap(_.population).sum / pays.size
      s"""    "$continent": $avg"""
    }.mkString(",\n")
    
    // Langues JSON
    val languesJson = langues.map { case (langue, nb) =>
      s"""    {"language": "$langue", "count": $nb}"""
    }.mkString(",\n")
    
    // Multilingues JSON
    val multiJson = multilingues.take(10).map { pays =>
      val langs = pays.languages.map(l => s""""$l"""").mkString(", ")
      s"""    {"name": "${pays.name.getOrElse("?")}", "languages": [$langs]}"""
    }.mkString(",\n")
    
    writerJson.println("{")
    writerJson.println(s"""  "statistics": {""")
    writerJson.println(s"""    "total_countries_parsed": ${nbClean + nbDirty + nbLarge},""")
    writerJson.println(s"""    "total_countries_valid": ${countries.size},""")
    writerJson.println(s"""    "duplicates_removed": $doublonsSup""")
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
  } catch {
    case e: Exception => println(s"Erreur : ${e.getMessage}")
  }
}

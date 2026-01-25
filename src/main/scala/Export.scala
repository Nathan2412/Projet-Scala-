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

def sauvegarderRapport(countries: List[Country], nbClean: Int, nbDirty: Int, nbLarge: Int): Unit = {
  try {
    val outputDir = new File("output")
    if (!outputDir.exists()) outputDir.mkdirs()
    
    val txtFile = new File("output/rapport.txt")
    val jsonFile = new File("output/rapport.json")
    if (txtFile.exists()) txtFile.delete()
    if (jsonFile.exists()) jsonFile.delete()
    
    val writerTxt = new PrintWriter("output/rapport.txt")
    
    writerTxt.println("=== RAPPORT - PAYS DU MONDE ===")
    writerTxt.println()
    writerTxt.println("SOURCES DE DONNEES :")
    writerTxt.println(s"- data_clean.json : $nbClean pays")
    writerTxt.println(s"- data_dirty.json : $nbDirty pays")
    writerTxt.println(s"- data_large.json : $nbLarge pays")
    writerTxt.println(s"- Total avant agregation : ${nbClean + nbDirty + nbLarge}")
    writerTxt.println(s"- Total apres agregation : ${countries.size}")
    writerTxt.println()
    
    val populationTotale = countries.flatMap(_.population).sum
    val superficieTotale = countries.flatMap(_.area).sum.toLong
    
    writerTxt.println("STATISTIQUES :")
    writerTxt.println(s"- Population totale : $populationTotale")
    writerTxt.println(s"- Superficie totale : $superficieTotale km2")
    writerTxt.println()
    
    val top10 = countries.filter(_.population.isDefined).sortBy(_.population.get).reverse.take(10)
    writerTxt.println("TOP 10 POPULATION :")
    top10.zipWithIndex.foreach { case (pays, i) =>
      writerTxt.println(s"${i+1}. ${pays.name.getOrElse("?")} : ${pays.population.get}")
    }
    writerTxt.println()
    
    val parContinent = countries.filter(_.continent.isDefined).groupBy(_.continent.get).toSeq.sortBy(_._1)
    writerTxt.println("PAR CONTINENT :")
    parContinent.foreach { case (continent, pays) =>
      writerTxt.println(s"- $continent : ${pays.size} pays")
    }
    
    writerTxt.close()
    
    val writerJson = new PrintWriter("output/rapport.json")
    
    val top10Json = top10.map { pays =>
      s"""    {"name": "${pays.name.getOrElse("?")}", "population": ${pays.population.get}}"""
    }.mkString(",\n")
    
    val continentJson = parContinent.map { case (continent, pays) =>
      s"""    {"continent": "$continent", "count": ${pays.size}}"""
    }.mkString(",\n")
    
    val paysJson = countries.take(20).map { pays =>
      val nom = pays.name.getOrElse("?").replace("\"", "\\\"")
      val pop = pays.population.getOrElse(0L)
      val cont = pays.continent.getOrElse("?")
      s"""    {"name": "$nom", "population": $pop, "continent": "$cont"}"""
    }.mkString(",\n")
    
    writerJson.println("{")
    writerJson.println(s"""  "sources": {""")
    writerJson.println(s"""    "data_clean": $nbClean,""")
    writerJson.println(s"""    "data_dirty": $nbDirty,""")
    writerJson.println(s"""    "data_large": $nbLarge,""")
    writerJson.println(s"""    "total_avant_agregation": ${nbClean + nbDirty + nbLarge},""")
    writerJson.println(s"""    "total_apres_agregation": ${countries.size}""")
    writerJson.println(s"""  },""")
    writerJson.println(s"""  "statistiques": {""")
    writerJson.println(s"""    "population_totale": $populationTotale,""")
    writerJson.println(s"""    "superficie_totale": $superficieTotale,""")
    writerJson.println(s"""    "nombre_pays": ${countries.size}""")
    writerJson.println(s"""  },""")
    writerJson.println(s"""  "top10_population": [""")
    writerJson.println(top10Json)
    writerJson.println(s"""  ],""")
    writerJson.println(s"""  "par_continent": [""")
    writerJson.println(continentJson)
    writerJson.println(s"""  ],""")
    writerJson.println(s"""  "pays": [""")
    writerJson.println(paysJson)
    writerJson.println(s"""  ]""")
    writerJson.println("}")
    
    writerJson.close()
    
    println("\nRapports sauvegardes dans output/rapport.txt et output/rapport.json")
  } catch {
    case e: Exception => println(s"Erreur : ${e.getMessage}")
  }
}

package countriesEtl

@main def main(): Unit = {
  println("=== ANALYSE DES PAYS ===")
  
  val debut = System.currentTimeMillis()
  val dataDir = "fp-scala-etl-project/1-countries"
  
  val fichiers = List(
    s"$dataDir/data_clean.json",
    s"$dataDir/data_dirty.json",
    s"$dataDir/data_large.json"
  )

  val resultat = for {
    tousLesPays <- chargerTousFichiers(fichiers)
    _ = println(s"\nEntrees totales : ${tousLesPays.size}")
    
    valides = nettoyerDonnees(tousLesPays)
    objetsInvalides = tousLesPays.size - valides.size
    _ = println(s"Objets invalides : $objetsInvalides")
    
    sansDoublons = enleverDoublons(valides)
    doublons = valides.size - sansDoublons.size
    _ = println(s"Doublons : $doublons")
    _ = println(s"Pays valides : ${sansDoublons.size}")
    
  } yield (sansDoublons, tousLesPays.size, objetsInvalides, doublons)

  resultat match {
    case Right((pays, total, invalides, doublons)) =>
      if (pays.isEmpty) {
        println("\nAucune donnee a analyser")
      } else {
        afficherStatistiques(pays)
        afficherTop5(pays)
        analyserParContinent(pays)
        
        val tempsMs = System.currentTimeMillis() - debut
        println(f"\nTemps : ${tempsMs / 1000.0}%.3f sec")
        
        sauvegarderRapport(pays, total, invalides, doublons, tempsMs)
        println("\nAnalyse terminee")
      }
      
    case Left(err) =>
      println(s"\nErreur : $err")
  }
}

def chargerTousFichiers(fichiers: List[String]): Either[String, List[Country]] = {
  val resultats = fichiers.map(chargerUnFichier)
  val pays = resultats.flatten
  if (pays.isEmpty) Left("Aucun fichier charge")
  else Right(pays)
}

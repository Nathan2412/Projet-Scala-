package countriesEtl

@main def main(): Unit =
  println("=== ANALYSE DES PAYS ===")
  val debut = System.currentTimeMillis()
  val dataDir = "fp-scala-etl-project/1-countries"

  val fichiers = List(
    s"$dataDir/data_clean.json",
    s"$dataDir/data_dirty.json",
    s"$dataDir/data_large.json"
  )

  // Traitement par fichier avec for-yield
  val resultatsParFichier = for
    chemin <- fichiers
    nomSource = extraireNomFichier(chemin)
    _ = println(s"\n--- Traitement de $nomSource ---")
    
    tousLesPays = chargerUnFichier(chemin)
    entreesTotales = tousLesPays.size
    _ = println(s"  Entrees : $entreesTotales")
    
    valides = nettoyerDonnees(tousLesPays)
    objetsInvalides = entreesTotales - valides.size
    
    sansDoublons = enleverDoublons(valides)
    doublons = valides.size - sansDoublons.size
    _ = println(s"  Valides : ${sansDoublons.size}")
    
    tempsMs = System.currentTimeMillis() - debut
    _ = sauvegarderRapportFichier(nomSource, sansDoublons, entreesTotales, objetsInvalides, doublons, tempsMs)
  yield (nomSource, tousLesPays, sansDoublons, entreesTotales, objetsInvalides, doublons)

  // Agregation globale
  println("\n=== AGREGATION GLOBALE ===")
  
  val tousLesPaysGlobal = resultatsParFichier.flatMap(_._2)
  val entreesTotalesGlobal = tousLesPaysGlobal.size
  println(s"Entrees totales : $entreesTotalesGlobal")

  val validesGlobal = nettoyerDonnees(tousLesPaysGlobal)
  val objetsInvalidesGlobal = entreesTotalesGlobal - validesGlobal.size
  println(s"Objets invalides : $objetsInvalidesGlobal")

  val sansDoublonsGlobal = enleverDoublons(validesGlobal)
  val doublonsGlobal = validesGlobal.size - sansDoublonsGlobal.size
  println(s"Doublons : $doublonsGlobal")
  println(s"Pays valides : ${sansDoublonsGlobal.size}")

  // Affichage stats
  if sansDoublonsGlobal.nonEmpty then
    println("\n=== TOP 5 POPULATION ===")
    topByPopulation(sansDoublonsGlobal, 5).zipWithIndex.foreach { case (pays, i) =>
      println(s"${i+1}. ${pays.name} : ${pays.value} habitants")
    }

    println("\n=== PAR CONTINENT ===")
    countByContinent(sansDoublonsGlobal).toSeq.sortBy(_._1).foreach { case (continent, nb) =>
      println(s"$continent : $nb pays")
    }

  val tempsTotal = System.currentTimeMillis() - debut
  println(f"\nTemps total : ${tempsTotal / 1000.0}%.3f sec")

  // Rapport global
  sauvegarderRapportGlobal(sansDoublonsGlobal, entreesTotalesGlobal, objetsInvalidesGlobal, doublonsGlobal, tempsTotal)

  println("\n=== ANALYSE TERMINEE ===")

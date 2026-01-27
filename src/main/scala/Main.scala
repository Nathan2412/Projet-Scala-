package countriesEtl

@main def main(): Unit = {
  println("=== ANALYSE DES PAYS ===")
  
  // Chronometre
  val debut = System.currentTimeMillis()
  
  val (clean, dirty, large) = analyserTousLesFichiers()
  
  analyserUnFichier("data_clean.json", clean)
  analyserUnFichier("data_dirty.json", dirty)
  analyserUnFichier("data_large.json", large)
  
  // Compter avant agregation
  val totalAvant = clean.size + dirty.size + large.size
  
  val paysAgreges = agregerDonnees(clean, dirty, large)
  
  // Calculer doublons supprimes
  val doublonsSup = totalAvant - paysAgreges.size
  
  if (paysAgreges.isEmpty) {
    println("Aucune donnee a analyser")
    return
  }
  
  afficherStatistiques(paysAgreges)
  afficherTop5(paysAgreges)
  analyserParContinent(paysAgreges)
  
  // Fin chronometre
  val tempsMs = System.currentTimeMillis() - debut
  val tempsSec = tempsMs / 1000.0
  println(f"\nTemps de traitement : $tempsSec%.3f secondes")
  
  sauvegarderRapport(paysAgreges, clean.size, dirty.size, large.size, doublonsSup, tempsMs)
  
  println("\nAnalyse terminee")
}

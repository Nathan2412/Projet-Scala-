package countriesEtl

@main def main(): Unit = {
  println("=== ANALYSE DES PAYS ===")
  
  val (clean, dirty, large) = analyserTousLesFichiers()
  
  analyserUnFichier("data_clean.json", clean)
  analyserUnFichier("data_dirty.json", dirty)
  analyserUnFichier("data_large.json", large)
  
  val paysAgreges = agregerDonnees(clean, dirty, large)
  
  if (paysAgreges.isEmpty) {
    println("Aucune donnee a analyser")
    return
  }
  
  afficherStatistiques(paysAgreges)
  afficherTop5(paysAgreges)
  analyserParContinent(paysAgreges)
  
  sauvegarderRapport(paysAgreges, clean.size, dirty.size, large.size)
  
  println("\nAnalyse terminee")
}

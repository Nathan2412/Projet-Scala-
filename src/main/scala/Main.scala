package countriesEtl

@main def main(): Unit = {
  val dataDir = "fp-scala-etl-project/1-countries"
  
  println("🌍 Chargement des données...")
  
  loadCountries(s"$dataDir/data_clean.json") match {
    case Right(countries) =>
      println(s"✅ ${countries.length} pays chargés avec succès")
      
      println("\n📊 Génération du rapport...")
      val report = generateReport(countries)
      
      // Écrire le rapport JSON
      writeJsonReport(report, "output/report.json") match {
        case Right(_) => println("✅ Rapport JSON écrit: output/report.json")
        case Left(err) => println(s"❌ Erreur: $err")
      }
      
      // Écrire le rapport texte
      writeTextReport(report, "output/report.txt") match {
        case Right(_) => println("✅ Rapport texte écrit: output/report.txt")
        case Left(err) => println(s"❌ Erreur: $err")
      }
      
      // Afficher le rapport
      println(generateTextContent(report))
      
    case Left(error) =>
      println(s"❌ Erreur lors du chargement: $error")
  }
}

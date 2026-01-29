package countriesEtl

def calculateStats(countries: List[Country]): CountryStats =
  val total = countries.length
  val totalPop = countries.flatMap(_.population).sum
  val totalArea = countries.flatMap(_.area).sum
  val avgPop = if total > 0 then totalPop.toDouble / total else 0.0
  val avgGdp = if total > 0 then countries.flatMap(_.gdp).sum / total else 0.0
  CountryStats(total, totalPop, totalArea, avgPop, avgGdp)

def topByPopulation(countries: List[Country], topN: Int = 10): List[TopCountry] =
  countries
    .filter(c => c.population.isDefined && c.name.isDefined)
    .sortBy(c => -c.population.get)
    .take(topN)
    .map(c => TopCountry(c.name.get, c.population.get, c.continent.getOrElse("?"), c.gdp))

def topByArea(countries: List[Country], topN: Int = 10): List[TopCountry] =
  countries
    .filter(c => c.area.isDefined && c.name.isDefined)
    .sortBy(c => -c.area.get)
    .take(topN)
    .map(c => TopCountry(c.name.get, c.area.get.toLong, c.continent.getOrElse("?"), c.gdp))

def topByGdp(countries: List[Country], topN: Int = 10): List[TopCountry] =
  countries
    .filter(c => c.gdp.isDefined && c.name.isDefined)
    .sortBy(c => -c.gdp.get)
    .take(topN)
    .map(c => TopCountry(c.name.get, c.gdp.get.toLong, c.continent.getOrElse("?"), c.gdp))

def countByContinent(countries: List[Country]): Map[String, Int] =
  countries.filter(_.continent.isDefined).groupBy(_.continent.get).view.mapValues(_.length).toMap

def mostSpokenLanguages(countries: List[Country], topN: Int = 10): Map[String, Int] =
  countries.flatMap(_.languages).groupBy(identity).view.mapValues(_.length).toList.sortBy(-_._2).take(topN).toMap

def paysMultilingues(countries: List[Country]): List[Country] =
  countries.filter(_.languages.length >= 3)

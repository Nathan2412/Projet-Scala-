package countriesEtl

def calculateStats(countries: List[Country]): CountryStats =
  val total = countries.length
  val totalPop = countries.map(_.population).sum
  val totalArea = countries.map(_.area).sum
  val avgPop = if total > 0 then totalPop.toDouble / total else 0.0
  val avgGdp = if total > 0 then countries.map(_.gdp).sum / total else 0.0
  CountryStats(total, totalPop, totalArea, avgPop, avgGdp)

def topByPopulation(countries: List[Country], topN: Int = 10): List[TopCountry] =
  countries
    .filter(c => c.population > 0 && c.name.nonEmpty)
    .sortBy(-_.population)
    .take(topN)
    .map(c => TopCountry(c.name, c.population, c.continent, c.gdp))

def topByArea(countries: List[Country], topN: Int = 10): List[TopCountry] =
  countries
    .filter(c => c.area > 0 && c.name.nonEmpty)
    .sortBy(-_.area)
    .take(topN)
    .map(c => TopCountry(c.name, c.area.toLong, c.continent, c.gdp))

def topByGdp(countries: List[Country], topN: Int = 10): List[TopCountry] =
  countries
    .filter(c => c.gdp > 0 && c.name.nonEmpty)
    .sortBy(-_.gdp)
    .take(topN)
    .map(c => TopCountry(c.name, c.gdp.toLong, c.continent, c.gdp))

def countByContinent(countries: List[Country]): Map[String, Int] =
  countries.filter(_.continent.nonEmpty).groupBy(_.continent).view.mapValues(_.length).toMap

def avgPopulationByContinent(countries: List[Country]): Map[String, Long] =
  countries
    .filter(_.continent.nonEmpty)
    .groupBy(_.continent)
    .view.mapValues(ctrs => ctrs.map(_.population).sum / ctrs.length)
    .toMap

def avgGdpByContinent(countries: List[Country]): Map[String, Double] =
  countries
    .filter(_.continent.nonEmpty)
    .groupBy(_.continent)
    .view.mapValues(ctrs => ctrs.map(_.gdp).sum / ctrs.length)
    .toMap

def mostSpokenLanguages(countries: List[Country], topN: Int = 10): Map[String, Int] =
  countries.flatMap(_.languages).groupBy(identity).view.mapValues(_.length).toList.sortBy(-_._2).take(topN).toMap

def currencyUsage(countries: List[Country], topN: Int = 10): Map[String, Int] =
  countries.filter(_.currency.nonEmpty).groupBy(_.currency).view.mapValues(_.length).toList.sortBy(-_._2).take(topN).toMap

def paysMultilingues(countries: List[Country]): List[Country] =
  countries.filter(_.languages.length >= 3)

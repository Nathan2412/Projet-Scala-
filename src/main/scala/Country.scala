package countriesEtl

import io.circe._
import io.circe.generic.auto._

case class Country(
    name: Option[String],
    code: String,
    capital: Option[String],
    continent: Option[String],
    population: Option[Long],
    area: Option[Double],
    gdp: Option[Double],
    languages: List[String],
    currency: Option[String]
)

case class TopCountry(
    name: String,
    value: Long,
    continent: String,
    gdp: Option[Double] = None
)

case class CountryStats(
    totalCountries: Int,
    totalPopulation: Long,
    totalArea: Double,
    averagePopulation: Double,
    averageGdp: Double
)

case class AnalysisReport(
    statistics: CountryStats,
    top10ByPopulation: List[TopCountry],
    top10ByArea: List[TopCountry],
    top10ByGdp: List[TopCountry],
    byContinent: Map[String, Int],
    avgPopulationByContinent: Map[String, Long],
    avgGdpByContinent: Map[String, Double],
    mostSpokenLanguages: Map[String, Int],
    currencyUsage: Map[String, Int]
)

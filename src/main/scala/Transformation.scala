package countriesEtl

def calculateStats(countries: List[Country]): CountryStats = {
    val total = countries.length
    val totalPop = countries.map(_.population).sum
    val totalArea = countries.flatMap(_.area).sum
    
    val avgPop = if (total > 0) totalPop.toDouble / total else 0.0
    val avgGdp = if (total > 0) countries.flatMap(_.gdp).sum / total else 0.0
    
    CountryStats(
        totalCountries = total,
        totalPopulation = totalPop,
        totalArea = totalArea,
        averagePopulation = avgPop,
        averageGdp = avgGdp
    )
}

def topByPopulation(countries: List[Country], topN: Int = 10): List[TopCountry] = {
    countries
        .sortBy(-_.population)
        .take(topN)
        .map(c => TopCountry(c.name, c.population, c.continent, c.gdp))
}

def topByArea(countries: List[Country], topN: Int = 10): List[TopCountry] = {
    countries
        .filter(_.area.isDefined)
        .sortBy(c => -c.area.get)
        .take(topN)
        .map(c => TopCountry(c.name, c.area.get.toLong, c.continent, c.gdp))
}

def topByGdp(countries: List[Country], topN: Int = 10): List[TopCountry] = {
    countries
        .filter(_.gdp.isDefined)
        .sortBy(c => -c.gdp.get)
        .take(topN)
        .map(c => TopCountry(c.name, c.gdp.get.toLong, c.continent, c.gdp))
}

def countByContinent(countries: List[Country]): Map[String, Int] = {
    countries
        .groupBy(_.continent)
        .view.mapValues(_.length).toMap
}

def avgPopulationByContinent(countries: List[Country]): Map[String, Long] = {
    countries
        .groupBy(_.continent)
        .view.mapValues { ctrs =>
            val totalPop = ctrs.map(_.population).sum
            totalPop / ctrs.length
        }.toMap
}

def avgGdpByContinent(countries: List[Country]): Map[String, Double] = {
    countries
        .groupBy(_.continent)
        .view.mapValues { ctrs =>
            val totalGdp = ctrs.flatMap(_.gdp).sum
            totalGdp / ctrs.length
        }.toMap
}

def mostSpokenLanguages(countries: List[Country], topN: Int = 10): Map[String, Int] = {
    countries
        .flatMap(_.languages)
        .groupBy(identity)
        .view.mapValues(_.length)
        .toList
        .sortBy(-_._2)
        .take(topN)
        .toMap
}

def currencyUsage(countries: List[Country], topN: Int = 10): Map[String, Int] = {
    countries
        .groupBy(_.currency)
        .view.mapValues(_.length)
        .toList
        .sortBy(-_._2)
        .take(topN)
        .toMap
}
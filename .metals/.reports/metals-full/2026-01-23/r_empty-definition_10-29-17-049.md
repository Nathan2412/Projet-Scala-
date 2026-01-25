error id: file:///C:/Users/artsm/Documents/BDML_2/scala/projet/src/main/scala/Transformation.scala:scala/collection/IterableOnceOps#sum().
file:///C:/Users/artsm/Documents/BDML_2/scala/projet/src/main/scala/Transformation.scala
empty definition using pc, found symbol in pc: 
empty definition using semanticdb
empty definition using fallback
non-local guesses:

offset: 1593
uri: file:///C:/Users/artsm/Documents/BDML_2/scala/projet/src/main/scala/Transformation.scala
text:
```scala
package countriesEtl

def calculStat(country: List[Country]) {
    val total = country.length
    var totalPop = country.map(_.population).sum
    var totalArea = country.map(_.area).sum

    if (total > 0) {
        val avgPop = totalPop / total else 0.0
        val avgGdp = country.map(_.gdp).sum / total else 0.0
    }
    countryStat(
        totalCountries = total,
        totalPopulation = totalPop,
        totalArea = totalArea
        averagePopulation = avgPop,
        averageGdp = avgGdp
    )
}

def byTopPopulation(country: List[Country], topN: Int = 10): List[TopCountry] = {
    country
        .sortBy(-_.population)
        .take(topN)
        .map(c => TopCountry(c.name, c.population, c.continent))
}

def byTopArea(country: List[Country], topN: Int = 10): List[TopCountry] = {
    country
        .filter(_.area.isDefined)
        .sortBy(c => -c.area.get)
        .take(topN)
        .map(c => TopCountry(c.name, c.area, c.continent))
}

def byTopGdp(country: List[Country], topN: Int = 10): List[TopCountry] = {
    country
        .filter(_.gdp.isDefined)
        .sortBy(c => -c.gdp.get)
        .take(topN)
        .map(c => TopCountry(c.name, c.gdp, c.continent))
}

def countByContinent(country: List[Country]): Map[String, Int] = {
    country
        .groupBy(_.continent)
        .mapValues(_.length)
}

def avgPopByContinent(country: List[Country]): Map[String, Double] = {
    country
        .groupBy(_.continent)
        .mapValues { countries =>
            val totalPop = countries.map(_.population).s@@um
            totalPop.toDouble / countries.length
        }
}
```


#### Short summary: 

empty definition using pc, found symbol in pc: 
# Projet ETL - Countries (Pays du monde)

## 📋 Dataset choisi

**1-countries** : Données géographiques et démographiques des pays du monde

Ce dataset contient des informations sur les pays : nom, code, capitale, continent, population, superficie, PIB, langues officielles et devise.

---

## 🚀 Comment compiler et exécuter

### Prérequis
- **Java** : JDK 11 ou supérieur
- **SBT** : Scala Build Tool (version 1.x)

### Compilation et exécution

```bash
# Se placer dans le dossier du projet
cd projet

# Compiler et exécuter
sbt run
```

### Fichiers de sortie

Après exécution, les résultats sont générés dans le dossier `output/` :
- `rapport.txt` : Rapport lisible avec toutes les statistiques
- `rapport.json` : Données structurées au format JSON

---

## 📁 Structure du projet

```
projet/
├── build.sbt                 # Configuration SBT et dépendances
├── README.md                 # Ce fichier
├── src/main/scala/
│   ├── Country.scala         # Case classes (modèles de données)
│   ├── Parsing.scala         # Lecture et parsing des fichiers JSON
│   ├── Transformation.scala  # Calculs et transformations statistiques
│   ├── Export.scala          # Génération des rapports de sortie
│   └── Main.scala            # Point d'entrée du programme
├── fp-scala-etl-project/1-countries/
│   ├── data_clean.json       # 100 pays, données parfaites
│   ├── data_dirty.json       # 500 pays avec erreurs
│   └── data_large.json       # 12 000+ entrées (test performance)
└── output/
    ├── rapport.txt           # Rapport texte généré
    └── rapport.json          # Résultats JSON générés
```

---

## 🔧 Explication des fichiers source

### Country.scala - Modèles de données

Définit les case classes qui représentent la structure des données :

```scala
case class Country(
    name: Option[String],      // Nom du pays (optionnel car peut manquer)
    code: String,              // Code pays (ex: "FR", "US")
    capital: Option[String],   // Capitale
    continent: Option[String], // Continent
    population: Option[Long],  // Population
    area: Option[Double],      // Superficie en km²
    gdp: Option[Double],       // PIB en milliards USD
    languages: List[String],   // Liste des langues officielles
    currency: Option[String]   // Devise
)
```

**Pourquoi `Option` ?** Les fichiers "dirty" contiennent des données manquantes. Avec `Option`, on peut représenter l'absence de valeur (`None`) sans crash.

### Parsing.scala - Lecture des fichiers

Fonctions pour charger les fichiers JSON avec la bibliothèque **Circe** :

- `loadCountries(filename)` : Charge un fichier JSON et retourne `Either[String, List[Country]]`
- Utilise `Try` pour gérer les erreurs de lecture fichier
- Utilise `decode` de Circe pour parser le JSON automatiquement

### Transformation.scala - Calculs statistiques

Fonctions de transformation utilisant les **HOFs** (Higher-Order Functions) :

| Fonction | Description | HOFs utilisées |
|----------|-------------|----------------|
| `calculateStats()` | Stats globales | `flatMap`, `sum` |
| `topByPopulation()` | Top 10 population | `filter`, `sortBy`, `take`, `map` |
| `topByArea()` | Top 10 superficie | `filter`, `sortBy`, `take`, `map` |
| `topByGdp()` | Top 10 PIB | `filter`, `sortBy`, `take`, `map` |
| `countByContinent()` | Pays par continent | `filter`, `groupBy`, `mapValues` |
| `avgPopulationByContinent()` | Moyenne pop/continent | `groupBy`, `flatMap`, `sum` |
| `mostSpokenLanguages()` | Langues répandues | `flatMap`, `groupBy`, `sortBy` |
| `paysMultilingues()` | Pays ≥3 langues | `filter` |

### Export.scala - Génération des rapports

Fonctions pour nettoyer les données et exporter les résultats :

- `nettoyerDonnees()` : Filtre les pays invalides (population ≤ 0, champs manquants)
- `enleverDoublons()` : Supprime les doublons par nom de pays
- `sauvegarderRapport()` : Génère les fichiers `rapport.txt` et `rapport.json`

### Main.scala - Point d'entrée

Orchestre le pipeline ETL complet :
1. Charge les 3 fichiers JSON
2. Nettoie et agrège les données
3. Calcule les statistiques
4. Exporte les rapports
5. Mesure le temps d'exécution

---

## 🛠️ Choix techniques

### Utilisation de `Option[T]`

Tous les champs qui peuvent être absents sont en `Option` :
```scala
// Accès sécurisé avec getOrElse
pays.name.getOrElse("Inconnu")

// Filtrage des valeurs présentes avec flatMap
countries.flatMap(_.population)  // Liste des populations non-nulles
```

### Utilisation de `Try[T]`

Pour la lecture de fichiers (peut échouer) :
```scala
val contentResult = Try {
  val source = Source.fromFile(filename)
  source.mkString
}
// Retourne Success(contenu) ou Failure(exception)
```

### HOFs (Higher-Order Functions)

| HOF | Utilisation | Exemple |
|-----|-------------|---------|
| `map` | Transformer chaque élément | `countries.map(_.name)` |
| `filter` | Garder selon condition | `countries.filter(_.population.isDefined)` |
| `flatMap` | Map + aplatir | `countries.flatMap(_.languages)` |
| `groupBy` | Regrouper par clé | `countries.groupBy(_.continent)` |
| `sortBy` | Trier | `countries.sortBy(_.population.get)` |
| `take` | Prendre les N premiers | `sorted.take(10)` |
| `fold/sum` | Agréger | `populations.sum` |

### Suppression des doublons

Le fichier `data_large.json` contient des doublons comme "China (1)", "China (2)", etc.

Solution avec regex :
```scala
def extraireNomBase(nom: Option[String]): String = {
  nom.getOrElse("").replaceAll("\\s*\\(\\d+\\)$", "")
}
// "China (42)" → "China"
```

---

## 📊 Statistiques calculées

| Statistique | Description |
|-------------|-------------|
| Total pays parsés | Nombre d'entrées lues dans les 3 fichiers |
| Pays valides | Après nettoyage et dédoublonnage |
| Doublons supprimés | Différence avant/après agrégation |
| Top 10 population | Les 10 pays les plus peuplés |
| Top 10 superficie | Les 10 pays les plus grands |
| Top 10 PIB | Les 10 pays les plus riches |
| Pays par continent | Nombre de pays par continent |
| Population moyenne/continent | Moyenne par continent |
| Langues répandues | Top 5 des langues officielles |
| Pays multilingues | Pays avec ≥3 langues officielles |

---

## ⏱️ Performance

| Fichier | Entrées | Temps |
|---------|---------|-------|
| data_clean.json | 100 | < 0.5s |
| data_dirty.json | 500 | < 0.5s |
| data_large.json | 12 120 | < 1s |
| **Total (3 fichiers)** | **12 721** | **~2 secondes** |

✅ Bien en dessous de l'objectif de 10 secondes

---

## 🐛 Difficultés rencontrées et solutions

### 1. Doublons dans data_large.json

**Problème** : Le fichier contenait des variations comme "China", "China (1)", "China (2)"...

**Solution** : Extraction du nom de base avec une regex, puis `groupBy` pour ne garder qu'une occurrence :
```scala
countries.groupBy(c => extraireNomBase(c.name)).map(_._2.head)
```

### 2. Champs manquants dans data_dirty.json

**Problème** : Certains pays n'avaient pas de PIB, capitale ou population.

**Solution** : Utilisation systématique de `Option` et filtrage avec `filter` et `flatMap` :
```scala
countries.filter(_.population.isDefined)  // Ignorer les pays sans population
countries.flatMap(_.gdp)                  // Liste des PIB non-nuls seulement
```

### 3. Valeurs invalides

**Problème** : Certaines entrées avaient une population négative ou nulle.

**Solution** : Validation dans `nettoyerDonnees()` :
```scala
countries.filter { pays =>
  pays.population.isDefined && pays.population.get > 0
}
```

---

## 📦 Dépendances

Définies dans `build.sbt` :

```scala
libraryDependencies ++= Seq(
  "io.circe" %% "circe-core" % "0.14.6",    // Types JSON
  "io.circe" %% "circe-generic" % "0.14.6", // Dérivation auto des codecs
  "io.circe" %% "circe-parser" % "0.14.6"   // Parsing JSON
)
```

---

## 👤 Auteur

- **Nom** : Artemiy Smogunov & Nathan Smadja - Tubiana 
- **Date** : Janvier 2026
- **Cours** : Programmation Fonctionnelle en Scala

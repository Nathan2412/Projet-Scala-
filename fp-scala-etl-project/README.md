# 📊 Données sources pour le projet final - Pipeline ETL

Ce répertoire contient **5 datasets différents** pour le projet final de Programmation Fonctionnelle en Scala.

## 🎯 Principe général

Chaque étudiant choisit **1 dataset** parmi les 5 proposés et doit créer **de zéro** un pipeline ETL complet :
- **Extract** : Parser les fichiers JSON avec Circe
- **Transform** : Calculer des statistiques avec les HOFs et monades
- **Load** : Exporter les résultats en JSON et texte

**Important** : Vous devrez analyser vous-même les fichiers JSON fournis et créer vos propres case classes. Les spécifications ne contiennent que la structure des outputs attendus, pas les valeurs finales.

## 📚 Prérequis

Ce projet mobilise **tous les concepts vus en cours** :
- ✅ **Immutabilité** : val, case classes
- ✅ **Fonctions d'ordre supérieur** : map, filter, flatMap, fold, groupBy, sortBy, etc.
- ✅ **Monades** : Option[T], Try[T], Either[L, R]
- ✅ **For-comprehension** : Composition de monades
- ✅ **Parsing JSON** : Circe (io.circe)
- ✅ **File I/O** : Lecture et écriture de fichiers

Si l'un de ces concepts n'est pas clair, **révisez les séances précédentes avant de commencer !**

## 📁 Structure

Chaque dataset contient :
- `SPECIFICATIONS.md` : Cahier des charges détaillé avec la structure des outputs attendus
- `data_clean.json` : 100 entrées parfaitement formatées (pour débuter)
- `data_dirty.json` : 500 entrées avec erreurs (champs manquants, valeurs nulles, doublons)
- `data_large.json` : 10 000+ entrées (test de performance)

## 🚀 Comment démarrer ?

### Étape 1 : Choisir votre dataset
Parcourez les 5 dossiers et lisez les `SPECIFICATIONS.md`. Choisissez le sujet qui vous intéresse le plus !

### Étape 2 : Analyser les données
Ouvrez `data_clean.json` de votre dataset choisi et analysez la structure JSON. Vous devrez créer une ou plusieurs case classes qui correspondent à cette structure.

### Étape 3 : Créer votre projet SBT
```bash
# Créez un nouveau projet Scala
mkdir mon-projet-etl
cd mon-projet-etl
```

Créez `build.sbt` avec les dépendances nécessaires (voir section Technologies).

### Étape 4 : Copier les fichiers de données
Copiez les 3 fichiers JSON de votre dataset choisi dans votre projet (par exemple dans `data/`).

### Étape 5 : Commencer le développement
1. Créez vos case classes en analysant le JSON
2. Implémentez le parsing avec Circe
3. Testez d'abord avec `data_clean.json`
4. Ajoutez la gestion d'erreurs pour `data_dirty.json`
5. Implémentez les transformations demandées
6. Exportez les résultats
7. Testez les performances avec `data_large.json`

## 🌍 Datasets disponibles

### 1. Countries - Pays du monde
**Dossier** : `1-countries/`  
**Thématique** : Données géographiques et démographiques  
**Exemples de stats** : Top pays par population, PIB moyen par continent, langues officielles

### 2. Football Players - Joueurs de football
**Dossier** : `2-football-players/`  
**Thématique** : Statistiques de joueurs professionnels  
**Exemples de stats** : Top buteurs, moyenne d'âge par poste, salaires par ligue

### 3. Movies - Films et séries
**Dossier** : `3-movies/`  
**Thématique** : Base de données type IMDb  
**Exemples de stats** : Films les mieux notés, acteurs prolifiques, genres populaires

### 4. Climate Events - Événements climatiques
**Dossier** : `4-climate-events/`  
**Thématique** : Catastrophes naturelles et données météo  
**Exemples de stats** : Événements par type, coûts des dégâts, zones à risque

### 5. League of Legends Champions
**Dossier** : `5-lol-champions/`  
**Thématique** : Champions et statistiques de gameplay  
**Exemples de stats** : Win rate par rôle, champions les plus bannis, pick rate

## 🎓 Consignes générales

### Partie 1 : Parsing et validation (40%)
- **Analyser** les fichiers JSON fournis et créer vos propres case classes
- **Parser** les 3 fichiers JSON avec Circe (`io.circe.parser._`)
- **Valider** les données selon les critères du `SPECIFICATIONS.md`
- **Gérer** les erreurs avec `Option`/`Try`/`Either` et for-comprehension
- **Logger** le nombre d'entrées lues, valides, erreurs et doublons supprimés

### Partie 2 : Transformations (40%)
- **Calculer** toutes les statistiques obligatoires demandées dans `SPECIFICATIONS.md`
- **Utiliser** les HOFs : `map`, `filter`, `flatMap`, `groupBy`, `sortBy`, `fold`, etc.
- **Composer** les opérations avec les for-comprehension quand approprié
- Les statistiques bonus sont optionnelles mais valorisées

### Partie 3 : Export (20%)
- **Générer** `results.json` avec les résultats (structure indiquée dans les specs)
- **Générer** `report.txt` avec un rapport lisible et bien formaté
- **Mesurer** et afficher le temps de traitement (code fourni dans les specs)
- Les fichiers de sortie doivent être dans un dossier `output/`

## 📦 Technologies recommandées

```scala
// build.sbt
libraryDependencies ++= Seq(
  "io.circe" %% "circe-core" % "0.14.6",
  "io.circe" %% "circe-generic" % "0.14.6",
  "io.circe" %% "circe-parser" % "0.14.6",
  "org.scalameta" %% "munit" % "0.7.29" % Test
)
```

## ⏱️ Durée

- **Séance 1** (3h30) : Parsing, validation, premières transformations
- **Séance 2** (3h30) : Transformations avancées, export, optimisation

## 📝 Rendu

**Deadline** : 1 semaine après la dernière séance  
**Format** : Archive ZIP nommée `nom_prenom_dataset.zip` (ex: `dupont_jean_countries.zip`)

**Contenu de l'archive** :
```
nom_prenom_dataset/
├── build.sbt
├── project/
│   └── build.properties
├── src/
│   └── main/
│       └── scala/
│           └── [votre code]
├── data/
│   ├── data_clean.json
│   ├── data_dirty.json
│   └── data_large.json
├── output/
│   ├── results.json        (généré par votre code)
│   └── report.txt          (généré par votre code)
└── README.md               (vos instructions)
```

**Votre README.md doit contenir** :
- Le dataset choisi
- Comment compiler et exécuter le projet
- Explication des choix techniques (case classes, gestion d'erreurs)
- Temps d'exécution obtenu sur `data_large.json`
- Difficultés rencontrées et solutions apportées

## 🎯 Critères d'évaluation

| Critère | Points |
|---------|--------|
| Structure du code & case classes | 3 pts |
| Gestion des erreurs (Option/Try/Either) | 3 pts |
| HOFs et transformations | 3 pts |
| Parsing JSON (Circe) | 3 pts |
| Résultats corrects | 3 pts |
| Gestion du fichier bruité | 1 pt |
| Performance (fichier large) | 1 pt |
| Documentation (README + commentaires) | 3 pts |
| **Total** | **20 pts** |

**Bonus** (+2 pts max) :
- Tests unitaires avec MUnit (+1pt)
- Statistiques supplémentaires créatives (+0.5pt)
- Gestion avancée des erreurs (+0.5pt)

## 💡 Conseils pratiques

### Pour le parsing
1. **Commencez par `data_clean.json`** : Assurez-vous que votre parsing fonctionne parfaitement
2. **Utilisez `io.circe.generic.auto._`** : Import automatique des encoders/decoders
3. **Testez avec une seule entrée d'abord** : Avant de parser tout le fichier
4. **N'oubliez pas les `Option`** : Pour les champs qui peuvent être manquants

### Pour la validation
1. **Créez des fonctions de validation** : Réutilisables et testables
2. **Utilisez for-comprehension** : Pour composer les validations avec `Either`
3. **Loggez toutes les erreurs** : Comptez parsing errors, duplicates, invalid data

### Pour les transformations
1. **Une transformation = une fonction** : Code modulaire et testable
2. **Vérifiez les cas limites** : Divisions par zéro, listes vides, etc.
3. **Utilisez les HOFs du cours** : `groupBy`, `sortBy`, `maxBy`, `fold`, etc.
4. **Testez chaque statistique individuellement** : Avant de tout exporter

### Pour l'optimisation
1. **`data_large.json` en dernier** : Optimisez seulement si nécessaire
2. **Évitez les transformations multiples** : Combinez les opérations quand possible
3. **L'objectif est < 10 secondes** : Sur un ordinateur moderne

### Pour la documentation
1. **Commentez les parties complexes** : Surtout les for-comprehension
2. **README clair** : Instructions pour compiler et exécuter
3. **Expliquez vos choix** : Structure des case classes, gestion d'erreurs


**Bon courage !** 🎓 N'hésitez pas à poser des questions pendant les séances de projet !


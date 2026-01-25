# 🌍 Dataset 1 : Countries - Pays du monde

## 📋 Description

Données géographiques, démographiques et économiques sur les pays du monde.

## 📊 Structure des données

### Fichiers fournis

- **data_clean.json** : 100 pays, données parfaites
- **data_dirty.json** : 500 pays avec :
  - Champs manquants (capital, gdp)
  - Valeurs nulles
  - Populations négatives
  - Doublons (même code pays)
  - Formats incohérents
- **data_large.json** : 12 000 entrées (pays + territoires + variations historiques)

## 🎯 Outputs attendus

### 1. Statistiques générales

**Fichier** : `results.json`

```json
{
  "statistics": {
    "total_countries_parsed": ...,
    "total_countries_valid": ...,
    "parsing_errors": ...,
    "duplicates_removed": ...
  },
  "top_10_by_population": [
    {
      "name": "...",
      "population": ...,
      "continent": "..."
    }
  ],
  "top_10_by_area": [...],
  "top_10_by_gdp": [...],
  "countries_by_continent": {
    "Africa": ...,
    "Asia": ...,
    "Europe": ...,
    "North America": ...,
    "South America": ...,
    "Oceania": ...
  },
  "average_population_by_continent": {
    "Africa": ...,
    "Asia": ...,
    ...
  },
  "most_common_languages": [
    {"language": "...", "count": ...}
  ],
  "multilingual_countries": [
    {"name": "...", "languages": ["...", "...", "..."]}
  ]
}
```

### 2. Rapport texte

**Fichier** : `report.txt`

```
===============================================
     RAPPORT D'ANALYSE - PAYS DU MONDE
===============================================

📊 STATISTIQUES DE PARSING
---------------------------
- Entrées totales lues      : ...
- Entrées valides           : ...
- Erreurs de parsing        : ...
- Doublons supprimés        : ...

🌍 RÉPARTITION PAR CONTINENT
-----------------------------
- Afrique                   : ... pays
- Asie                      : ... pays
- Europe                    : ... pays
- Amérique du Nord          : ... pays
- Amérique du Sud           : ... pays
- Océanie                   : ... pays

👥 TOP 10 - POPULATION
----------------------
1. ...                      : ... hab.
2. ...                      : ... hab.
...

🗺️  TOP 10 - SUPERFICIE
-----------------------
1. ...                      : ... km²
2. ...                      : ... km²
...

💰 TOP 10 - PIB
---------------
1. ...                      : ... milliards USD
2. ...                      : ... milliards USD
...

🗣️  LANGUES LES PLUS RÉPANDUES
--------------------------------
1. ...                      : ... pays
2. ...                      : ... pays
...

📈 MOYENNES PAR CONTINENT
--------------------------
- Afrique                   : ... hab. (moyenne)
- Asie                      : ... hab. (moyenne)
- Europe                    : ... hab. (moyenne)
...

⏱️  PERFORMANCE
---------------
- Temps de traitement       : ... secondes
- Entrées/seconde           : ...

===============================================
```

## 🔧 Transformations à implémenter

### Obligatoires

1. **Parsing et validation**
   - Parser les 3 fichiers
   - Valider : population > 0, area > 0, code non vide
   - Supprimer les doublons (par code pays)

2. **Top 10**
   - Top 10 pays par population
   - Top 10 pays par superficie
   - Top 10 pays par PIB (filtrer ceux qui ont un PIB)

3. **Agrégations**
   - Compter pays par continent
   - Population moyenne par continent
   - Langues les plus répandues (top 5)

4. **Filtres**
   - Pays multilingues (>= 3 langues officielles)

### Bonus (optionnel)

5. **Densité de population**
   - Calculer densité = population / area
   - Top 10 pays les plus denses

6. **Richesse par habitant**
   - Calculer PIB/habitant
   - Top 10 pays les plus riches par habitant

## 💡 Conseil - Mesure de performance

```scala
val start = System.currentTimeMillis()
// ... traitement
val duration = (System.currentTimeMillis() - start) / 1000.0
println(f"Traitement effectué en $duration%.3f secondes")
```

## ✅ Critères de réussite

- ✅ Les 3 fichiers sont parsés sans erreur fatale
- ✅ Les données invalides sont filtrées (pas de crash)
- ✅ Les 10 statistiques obligatoires sont présentes
- ✅ `results.json` est valide et bien formaté
- ✅ `report.txt` est lisible et structuré
- ✅ Le traitement de `data_large.json` prend < 10 secondes



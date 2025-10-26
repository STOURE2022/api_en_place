# 🎉 NOUVELLES FEATURES R1 - Documentation

## 📋 Vue d'Ensemble

Ce document détaille les **5 nouvelles features prioritaires** implémentées pour la Release 1 (R1).

---

## ✅ 1. NON-ZIPPED FILE HANDLING

### 🎯 Objectif

Gérer les fichiers CSV/Parquet/JSON directs (non-ZIP) en plus des archives ZIP.

### 📖 Spécification Gherkin

```gherkin
Scenario: Non-zipped file received
Given a file is received
And the file is not a zip archive
When the file is processed
Then the file is copied to the raw destination folder
```

### 🔧 Implémentation

**Fichier:** `file_handler.py`

**Fonctionnalités:**
- ✅ Détection automatique fichiers non-ZIP
- ✅ Support formats: .csv, .parquet, .json, .txt
- ✅ Copie vers `/extracted/<table_name>/`
- ✅ Organisation automatique par table
- ✅ Gestion erreurs

### 📝 Utilisation

```python
from file_handler import FileHandler

# Initialiser
handler = FileHandler(spark, config)

# Lister fichiers non-ZIP
files = handler.list_non_zip_files()
print(f"Fichiers trouvés: {files}")

# Traiter tous les fichiers
result = handler.process_non_zip_files()

# Résultat
# {
#   "status": "SUCCESS",
#   "file_count": 5,
#   "failed_count": 0,
#   "results": [...]
# }
```

### ⚙️ Configuration

```json
{
  "non_zip_support_enabled": true
}
```

### 📊 Workflow

```
📂 input/
├── customers.csv          ← Non-ZIP
├── orders.zip             ← ZIP
└── products.parquet       ← Non-ZIP
    ↓
📂 extracted/
├── customers/
│   └── customers.csv      ✅ Copié
├── orders/
│   ├── order_001.csv      ✅ Extrait ZIP
│   └── order_002.csv      ✅ Extrait ZIP
└── products/
    └── products.parquet   ✅ Copié
```

---

## ✅ 2. DUPLICATE PREVENTION

### 🎯 Objectif

Empêcher le traitement d'un fichier déjà ingéré.

### 📖 Spécification Gherkin

```gherkin
Scenario: Prevent processing of the same file
Given file name received and all filenames ingested
When file name exists in all filenames ingested
Then Reject the file, Raise error "000000002"
```

### 🔧 Implémentation

**Fichier:** `tracking_manager.py`

**Fonctionnalités:**
- ✅ Table de tracking: `wax_processed_files`
- ✅ Vérification avant traitement
- ✅ Error code: `000000002`
- ✅ Historique complet

### 📝 Utilisation

```python
from tracking_manager import TrackingManager

# Initialiser
tracking = TrackingManager(spark, config)

# Vérifier duplicate
result = tracking.check_duplicate(
    table_name="customers",
    filename="customers_20251016.csv"
)

if result["is_duplicate"]:
    print(f"❌ Erreur {result['error_code']}: {result['message']}")
    # File customers_20251016.csv already processed
else:
    # Traiter fichier...
    
    # Enregistrer après traitement
    tracking.register_file(
        table_name="customers",
        filename="customers_20251016.csv",
        status="SUCCESS",
        row_count=1000
    )
```

### ⚙️ Configuration

```json
{
  "duplicate_check_enabled": true
}
```

### 📊 Table de Tracking

```sql
-- Structure: wax_processed_files
CREATE TABLE wax_processed_files (
    table_name STRING NOT NULL,
    filename STRING NOT NULL,
    file_date TIMESTAMP,
    processed_date TIMESTAMP NOT NULL,
    status STRING NOT NULL,  -- SUCCESS ou FAILED
    row_count STRING
)
```

### 🔍 Requêtes Utiles

```python
# Afficher résumé
tracking.display_tracking_summary()

# Lister fichiers traités
files = tracking.get_processed_files(table_name="customers")

# Nettoyer anciens
tracking.cleanup_old_records(days_to_keep=90)
```

---

## ✅ 3. FILE ORDER VALIDATION

### 🎯 Objectif

Vérifier que les fichiers arrivent dans l'ordre chronologique.

### 📖 Spécification Gherkin

```gherkin
Scenario: Prevent processing of old file
Given date in filename and previous date
When date < previous date
Then Reject file, Raise error "000000003"
```

### 🔧 Implémentation

**Fichier:** `tracking_manager.py` (même classe)

**Fonctionnalités:**
- ✅ Extraction date depuis filename
- ✅ Comparaison avec dernière date
- ✅ Error code: `000000003`
- ✅ Support patterns multiples

### 📝 Utilisation

```python
# Vérifier ordre
result = tracking.check_file_order(
    table_name="customers",
    filename="customers_20251015.csv"  # Date < dernière
)

if not result["valid_order"]:
    print(f"❌ Erreur {result['error_code']}: {result['message']}")
    # File date 2025-10-15 < last processed 2025-10-16
```

### ⚙️ Configuration

```json
{
  "file_order_check_enabled": true
}
```

### 🔤 Patterns Filename Supportés

```
✅ filename_yyyymmdd_hhmmss.csv   → 20251016_101112
✅ filename_yyyymmdd.csv           → 20251016
✅ yyyymmdd_filename.csv           → 20251016_filename
✅ filename_yyyy-mm-dd.csv         → 2025-10-16
✅ filename_yyyy_mm_dd.csv         → 2025_10_16
```

### 📊 Validation Combinée

```python
# Validation complète (duplicate + ordre)
validation = tracking.validate_file(
    table_name="customers",
    filename="customers_20251016.csv"
)

if not validation["valid"]:
    for error in validation["errors"]:
        print(f"❌ {error['type']}: {error['message']}")
        print(f"   Code: {error['error_code']}")
```

---

## ✅ 4. LAST VS ALL TABLES

### 🎯 Objectif

Créer deux tables distinctes selon le mode d'ingestion :
- `<table>_last` : Dernières données
- `<table>_all` : Historique complet

### 📖 Spécification Gherkin

```gherkin
Scenario: FULL_SNAPSHOT mode
Given ingestion mode is "FULL_SNAPSHOT"
Then create or update "<last_table>" containing new file
And create or update "<all_table>" containing all files

Scenario: DELTA_FROM_FLOW mode
Given ingestion mode is "DELTA_FROM_FLOW"
Then create or update "<all_table>" ONLY (no last table)
```

### 🔧 Implémentation

**Fichier:** `ingestion_enhanced.py`

**Fonctionnalités:**
- ✅ Gestion automatique Last/All
- ✅ Noms personnalisables
- ✅ Conformité stricte aux specs
- ✅ 5 modes d'ingestion

### 📝 Utilisation

```python
from ingestion_enhanced import IngestionManagerEnhanced

# Initialiser
ingestion = IngestionManagerEnhanced(spark, config, delta_manager)

# Mode FULL_SNAPSHOT: crée Last + All
stats = ingestion.apply_ingestion_mode(
    df_raw=df,
    column_defs=column_defs,
    table_name="customers",
    ingestion_mode="FULL_SNAPSHOT",
    file_name_received="customers.csv"
)

# Résultat:
# • customers_last ✅ créé (overwrite)
# • customers_all  ✅ créé (append)

# Mode DELTA_FROM_FLOW: All uniquement
stats = ingestion.apply_ingestion_mode(
    df_raw=df,
    column_defs=column_defs,
    table_name="orders",
    ingestion_mode="DELTA_FROM_FLOW",
    file_name_received="orders.csv"
)

# Résultat:
# • orders_last ❌ NON créé
# • orders_all  ✅ créé (append)
```

### 📊 Modes d'Ingestion

| Mode | Last Table | All Table | Description |
|------|------------|-----------|-------------|
| **FULL_SNAPSHOT** | ✅ Overwrite | ✅ Append | Snapshot complet |
| **DELTA_FROM_FLOW** | ❌ Pas créé | ✅ Append | Delta uniquement |
| **DELTA_FROM_NON_HISTORIZED** | ✅ Merge | ✅ Append | Merge sur clés |
| **DELTA_FROM_HISTORIZED** | ✅ Append | ✅ Append | Historique SCD2 |
| **FULL_KEY_REPLACE** | ✅ Delete+Insert | ✅ Append | Remplacement clés |

### ⚙️ Configuration Excel

```
File-Table Sheet:
┌─────────────────────┬───────────────────┐
│ Delta Table Name    │ customers         │
│ Last Table Name     │ customers_latest  │  ← Personnalisé (optionnel)
│ Ingestion mode      │ FULL_SNAPSHOT     │
└─────────────────────┴───────────────────┘

Si Last Table Name vide → <table>_last par défaut
```

### 🔍 Vérification

```python
# Vérifier tables créées
spark.sql("SHOW TABLES IN catalog.schema").show()

# customers_all   ✅
# customers_last  ✅

# Compter lignes
all_count = spark.table("customers_all").count()
last_count = spark.table("customers_last").count()

print(f"All: {all_count}, Last: {last_count}")
```

---

## ✅ 5. INVALID LINES TABLE

### 🎯 Objectif

Sauvegarder les lignes rejetées dans une table dédiée pour audit et debug.

### 📖 Spécification Gherkin

```gherkin
Rule: Invalid Lines Generate Rule
When Invalid lines generated is true
Then keep all invalid lines in a Table
```

### 🔧 Implémentation

**Fichier:** `invalid_lines_manager.py`

**Fonctionnalités:**
- ✅ Table: `<table>_invalid_lines`
- ✅ Métadonnées complètes
- ✅ Raisons de rejet
- ✅ Nettoyage automatique

### 📝 Utilisation

```python
from invalid_lines_manager import InvalidLinesManager

# Initialiser
invalid_mgr = InvalidLinesManager(spark, config)

# Sauvegarder lignes invalides
invalid_mgr.save_invalid_lines(
    df=invalid_df,
    table_name="customers",
    filename="customers_20251016.csv",
    rejection_reason="Validation failed",
    error_type="NULL_VALUE",
    error_details="customer_id is NULL"
)

# Afficher résumé
invalid_mgr.get_invalid_lines_summary("customers")

# Compter
count = invalid_mgr.get_invalid_lines_count("customers")
print(f"Total lignes invalides: {count}")
```

### ⚙️ Configuration

```json
{
  "invalid_lines_generated": true,
  "invalid_lines_cleanup_days": 30
}
```

### 📊 Structure Table

```sql
-- Structure: <table>_invalid_lines
CREATE TABLE customers_invalid_lines (
    invalid_line_id BIGINT NOT NULL,
    filename STRING NOT NULL,
    rejection_date TIMESTAMP NOT NULL,
    rejection_reason STRING NOT NULL,
    error_type STRING NOT NULL,
    error_details STRING,
    line_number BIGINT,
    raw_data STRING,
    -- + toutes les colonnes de la table source
    customer_id INT,
    name STRING,
    email STRING,
    ...
)
```

### 🔍 Requêtes Utiles

```sql
-- Lignes invalides par type d'erreur
SELECT error_type, COUNT(*) as count
FROM customers_invalid_lines
GROUP BY error_type
ORDER BY count DESC;

-- Dernières erreurs
SELECT filename, rejection_reason, rejection_date
FROM customers_invalid_lines
ORDER BY rejection_date DESC
LIMIT 10;

-- Erreurs par fichier
SELECT filename, COUNT(*) as errors
FROM customers_invalid_lines
GROUP BY filename
ORDER BY errors DESC;
```

### 🧹 Nettoyage

```python
# Nettoyer anciennes lignes (> 30 jours)
invalid_mgr.cleanup_invalid_lines(
    table_name="customers",
    days_to_keep=30
)
```

---

## ✅ BONUS: FAIL FAST OPTION

### 🎯 Objectif

Arrêter l'ingestion immédiatement si le seuil d'erreurs est atteint.

### 📖 Spécification Gherkin

```gherkin
Scenario: Stop ingestion when threshold reached
Given error percentage threshold
When percentage of errors >= threshold
Then stop ingestion immediately
And Reject file, Raise error "000000007"
```

### 🔧 Implémentation

**Fichier:** `column_processor.py` (intégré)

### ⚙️ Configuration

```json
{
  "fail_fast_enabled": true,
  "fail_fast_threshold": 10
}
```

### 📝 Utilisation

```python
# Dans main_enhanced.py
api_params = {
    "fail_fast_enabled": True,
    "fail_fast_threshold": 10  # 10% d'erreurs max
}

pipeline = WAXPipelineEnhanced(spark, config, api_params)
```

### 📊 Comportement

```
Lignes lues : 100
Lignes erreur: 11
Seuil       : 10%

→ 11/100 = 11% >= 10%
→ ❌ ARRÊT IMMÉDIAT
→ Error code: 000000007
```

---

## 📋 CONFIGURATION COMPLÈTE

### config_params_enhanced.json

```json
{
  "dev": {
    "catalog": "abu_catalog",
    "schema_files": "databricksassetbundletest",
    "volume": "externalvolumetes",
    "schema_tables": "gdp_poc_dev",
    "env": "dev",
    "version": "v2.0.0",
    
    "fail_fast_enabled": true,
    "fail_fast_threshold": 10,
    "invalid_lines_generated": true,
    "duplicate_check_enabled": true,
    "file_order_check_enabled": true,
    "non_zip_support_enabled": true,
    "delete_zip_after_extract": true,
    "input_header_mode": "empty",
    "ict_default": 10,
    "rlt_default": 10,
    "partitioning_default": "yyyy/mm/dd",
    "tracking_cleanup_days": 90,
    "invalid_lines_cleanup_days": 30
  }
}
```

---

## 🚀 PIPELINE COMPLET

### main_enhanced.py

```python
from main_enhanced import WAXPipelineEnhanced

# Initialiser
pipeline = WAXPipelineEnhanced(spark, config, api_params)

# Exécuter
stats = pipeline.run(config_excel="/path/to/config.xlsx")

# Workflow complet:
# 1. ✅ Traitement fichiers non-ZIP
# 2. ✅ Extraction ZIP
# 3. ✅ Validation tracking (duplicate + ordre)
# 4. ✅ Auto Loader
# 5. ✅ Validation données
# 6. ✅ Sauvegarde lignes invalides
# 7. ✅ Ingestion (Last + All selon mode)
# 8. ✅ Logs et métriques
```

---

## 📊 NOUVEAUX FICHIERS

```
src/
├── tracking_manager.py           ✅ Duplicate + File order
├── file_handler.py               ✅ Non-ZIP files
├── invalid_lines_manager.py      ✅ Invalid lines table
├── ingestion_enhanced.py         ✅ Last/All tables
└── main_enhanced.py              ✅ Pipeline complet

config_api/
└── config_params_enhanced.json   ✅ Nouveaux paramètres
```

---

## ✅ CHECKLIST VALIDATION

### Tests Unitaires

```bash
# Test tracking
python tracking_manager.py

# Test file handler
python file_handler.py

# Test invalid lines
python invalid_lines_manager.py

# Test ingestion
python ingestion_enhanced.py
```

### Tests d'Intégration

```bash
# Pipeline complet
python main_enhanced.py
```

### Validation Features

- [ ] Non-ZIP files copiés correctement
- [ ] Duplicate détecté (error 000000002)
- [ ] File order validé (error 000000003)
- [ ] Tables Last/All créées selon mode
- [ ] Lignes invalides sauvegardées
- [ ] Fail fast fonctionne
- [ ] Logs complets

---

## 📚 DOCUMENTATION ADDITIONNELLE

- `FEATURES_COVERAGE_ANALYSIS.md` : Analyse complète couverture
- `SPECIFICATIONS_MAPPING.md` : Mapping specs → code
- `README.md` : Documentation générale

---

## 🎉 RÉSUMÉ

### Avant (v1.0.0)

❌ Seulement ZIP supportés
❌ Pas de duplicate prevention
❌ Pas de file order validation
❌ Une seule table créée
❌ Pas de table invalid lines
❌ Fail fast non configurable

### Après (v2.0.0)

✅ Non-ZIP + ZIP supportés
✅ Duplicate prevention (000000002)
✅ File order validation (000000003)
✅ Last + All tables selon mode
✅ Table invalid lines complète
✅ Fail fast configurable

---

**Version:** 2.0.0  
**Date:** Octobre 2025  
**Statut:** ✅ Production Ready

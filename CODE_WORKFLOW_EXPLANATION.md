# 🔄 FONCTIONNEMENT GÉNÉRAL DU CODE WAX v2.0.0

## 📋 Vue d'Ensemble

Le pipeline WAX est un système d'ingestion de données qui traite des fichiers (ZIP ou directs) et les charge dans des tables Delta Lake avec validation et traçabilité complètes.

---

## 🎯 WORKFLOW GLOBAL - Vue Simplifiée

```
📂 Fichiers Arrivants
     ↓
[1] Détection Type (ZIP vs Non-ZIP)
     ↓
[2] Extraction/Copie → /extracted/
     ↓
[3] Validation Tracking (Duplicate + Ordre)
     ↓
[4] Lecture Données (Auto Loader)
     ↓
[5] Validation Données (Colonnes, Types, Nulls)
     ↓
[6] Traitement Colonnes (Transformations)
     ↓
[7] Gestion Erreurs (Invalid Lines)
     ↓
[8] Ingestion Delta Lake (Last + All)
     ↓
[9] Logs & Métriques
     ↓
✅ Tables Delta Prêtes
```

---

## 🚀 WORKFLOW DÉTAILLÉ - Étape par Étape

### 📁 PHASE 1: ARRIVÉE FICHIERS

```
/Volumes/catalog/schema/volume/input/zip/
├── customers.csv              ← Fichier direct (non-ZIP)
├── orders.zip                 ← Archive ZIP
└── products_20251016.csv      ← Fichier avec date
```

**Ce qui se passe:**
- Les fichiers sont déposés dans `/input/zip/`
- Le pipeline détecte automatiquement leur présence
- Démarre le traitement

---

### 🔍 PHASE 2: DÉTECTION & EXTRACTION

#### Module: `file_handler.py` + `unzip_module.py`

```
┌─────────────────────────────────────────┐
│  FICHIER ARRIVÉ                         │
└─────────────┬───────────────────────────┘
              │
              ↓
         [Est-ce un ZIP?]
              │
       ┌──────┴──────┐
       │             │
     OUI            NON
       │             │
       ↓             ↓
  [unzip_module]  [file_handler]
  Extraction      Copie directe
  dans ZIP        
       │             │
       └──────┬──────┘
              ↓
     /extracted/<table>/
     ├── file1.csv
     ├── file2.csv
     └── file3.csv
```

**Exemple concret:**

```python
# customers.csv (non-ZIP)
file_handler.process_non_zip_files()
→ Copie vers: /extracted/customers/customers.csv

# orders.zip (ZIP contenant order_001.csv, order_002.csv)
unzip_module.process_zips()
→ Extrait vers: /extracted/orders/order_001.csv
→ Extrait vers: /extracted/orders/order_002.csv
→ Supprime: orders.zip (si configured)
```

---

### 🛡️ PHASE 3: VALIDATION TRACKING

#### Module: `tracking_manager.py`

```
┌─────────────────────────────────────────┐
│  FICHIER EXTRAIT                        │
│  customers_20251016.csv                 │
└─────────────┬───────────────────────────┘
              │
              ↓
     [CHECK DUPLICATE]
     Table: wax_processed_files
              │
       ┌──────┴──────┐
       │             │
   EXISTE         NOUVEAU
       │             │
       ↓             ↓
   ❌ REJECT      ✅ CONTINUE
   Error 000000002    │
                      ↓
              [CHECK FILE ORDER]
              Date dans filename vs
              dernière date traitée
                      │
               ┌──────┴──────┐
               │             │
          DATE < LAST    DATE ≥ LAST
               │             │
               ↓             ↓
           ❌ REJECT      ✅ CONTINUE
           Error 000000003
```

**Exemple concret:**

```python
# tracking_manager valide le fichier
validation = tracking_manager.validate_file(
    table_name="customers",
    filename="customers_20251016.csv"
)

if not validation["valid"]:
    print("Fichier rejeté:")
    for error in validation["errors"]:
        print(f"  - {error['error_code']}: {error['message']}")
    # STOP - ne pas traiter ce fichier
else:
    # OK - continuer le traitement
    pass
```

---

### 📖 PHASE 4: LECTURE DONNÉES

#### Module: `autoloader_module.py`

```
┌─────────────────────────────────────────┐
│  FICHIER VALIDÉ                         │
│  /extracted/customers/                  │
│    customers_20251016.csv               │
└─────────────┬───────────────────────────┘
              │
              ↓
      [AUTO LOADER]
      - Lecture CSV/Parquet/JSON
      - Inférence schéma (optionnel)
      - Parsing selon délimiteur
              │
              ↓
      [DataFrame PySpark]
      +-------------+--------+
      | customer_id | name   |
      +-------------+--------+
      | 1           | Alice  |
      | 2           | Bob    |
      | NULL        | Charlie|  ← Problème potentiel
      +-------------+--------+
```

**Exemple concret:**

```python
# Auto Loader charge le fichier
df = autoloader.load_file(
    file_path="/extracted/customers/customers_20251016.csv",
    file_format="csv",
    delimiter=","
)

print(f"Lignes lues: {df.count()}")
# Lignes lues: 1000
```

---

### ✅ PHASE 5: VALIDATION DONNÉES

#### Module: `validator.py`

```
┌─────────────────────────────────────────┐
│  DataFrame Chargé                       │
└─────────────┬───────────────────────────┘
              │
              ↓
   [VALIDATION 1: Colonnes]
   Toutes colonnes présentes?
   Types corrects?
              │
       ┌──────┴──────┐
       │             │
     NON            OUI
       │             │
       ↓             ↓
   ❌ REJECT      [VALIDATION 2: Nullables]
   Error 000000006  Colonnes NOT NULL
                    ont-elles des NULL?
                         │
                  ┌──────┴──────┐
                  │             │
                OUI            NON
                  │             │
                  ↓             ↓
              [ERROR ACTION]  ✅ CONTINUE
              ICT_DRIVEN/         │
              REJECT/             ↓
              LOG_ONLY      [VALIDATION 3: RLT]
                            % lignes rejetées
                            < seuil?
                                 │
                          ┌──────┴──────┐
                          │             │
                        NON            OUI
                          │             │
                          ↓             ↓
                      ❌ REJECT      ✅ CONTINUE
                      Fichier        Au traitement
                      entier
```

**Exemple concret:**

```python
# Validation du DataFrame
validation_result = validator.validate_data(
    df=df,
    column_defs=column_defs,
    filename="customers_20251016.csv",
    ict_threshold=10,  # 10% colonnes invalides par ligne max
    rlt_threshold=10   # 10% lignes rejetées par fichier max
)

if not validation_result["valid"]:
    print(f"Validation échouée: {validation_result['errors']}")
    # STOP - fichier rejeté
else:
    print("Validation OK - continuer")
```

---

### 🔧 PHASE 6: TRAITEMENT COLONNES

#### Module: `column_processor.py`

```
┌─────────────────────────────────────────┐
│  DataFrame Validé                       │
└─────────────┬───────────────────────────┘
              │
              ↓
     [Pour chaque colonne]
              │
              ↓
   [Transformation Type?]
              │
       ┌──────┴──────────────┐
       │                     │
     DATE              REGEX/AUTRE
       │                     │
       ↓                     ↓
 [Transformation Date]  [Autres transfo]
 Pattern: dd/MM/yyyy    
 → yyyy-MM-dd HH:mm:ss  
       │                     │
       └──────┬──────────────┘
              │
              ↓
      [Error Action?]
      Si erreur transformation
              │
       ┌──────┴──────┬──────┐
       │             │      │
   ICT_DRIVEN    REJECT   LOG_ONLY
       │             │      │
       ↓             ↓      ↓
   Calcul ICT    Rejeter  Logger
   Si > seuil    ligne    seulement
   → Rejeter              │
   ligne                  │
       │                  │
       └──────┬───────────┘
              │
              ↓
   [DataFrame Traité]
   avec colonnes transformées
```

**Exemple concret:**

```python
# Traitement des colonnes
df_processed = column_processor.process_columns(
    df=df,
    column_defs=column_defs
)

# Exemple transformation date
# Avant: "15/10/2025"
# Après: "2025-10-15 00:00:00"

# Colonne _validation_failed ajoutée
# True = ligne invalide, False = ligne valide
```

---

### 🗂️ PHASE 7: GESTION LIGNES INVALIDES

#### Module: `invalid_lines_manager.py`

```
┌─────────────────────────────────────────┐
│  DataFrame Traité                       │
│  avec colonne _validation_failed        │
└─────────────┬───────────────────────────┘
              │
              ↓
   [Config: invalid_lines_generated?]
              │
       ┌──────┴──────┐
       │             │
     TRUE          FALSE
       │             │
       ↓             └──→ [Ignorer lignes invalides]
   [Filtrer                      │
   _validation_failed = true]     │
       │                          │
       ↓                          │
   [Sauvegarder dans              │
   <table>_invalid_lines]         │
   + métadonnées complètes        │
       │                          │
       └──────┬───────────────────┘
              │
              ↓
   [Garder seulement
   _validation_failed = false]
              │
              ↓
   [DataFrame Nettoyé]
   Prêt pour ingestion
```

**Exemple concret:**

```python
# Séparer lignes valides et invalides
invalid_df = df_processed.filter("_validation_failed = true")
valid_df = df_processed.filter("_validation_failed = false")

# Sauvegarder lignes invalides
if config["invalid_lines_generated"] and invalid_df.count() > 0:
    invalid_lines_manager.save_invalid_lines(
        df=invalid_df,
        table_name="customers",
        filename="customers_20251016.csv",
        rejection_reason="Validation failed",
        error_type="DATA_VALIDATION"
    )
    # → Sauvegardé dans: customers_invalid_lines

# Continuer avec lignes valides uniquement
df = valid_df
```

---

### 💾 PHASE 8: INGESTION DELTA LAKE

#### Module: `ingestion_enhanced.py` + `delta_manager.py`

```
┌─────────────────────────────────────────┐
│  DataFrame Nettoyé (lignes valides)     │
└─────────────┬───────────────────────────┘
              │
              ↓
   [Quel mode d'ingestion?]
              │
    ┌─────────┼─────────┬─────────┐
    │         │         │         │
FULL_SNAPSHOT DELTA_   DELTA_    Autres
              FROM_    NON_HIS   modes
              FLOW     TORIZED
    │         │         │         │
    ↓         ↓         ↓         ↓
    
[FULL_SNAPSHOT]
├─→ Table Last (overwrite)
│   customers_last ✅
│   Dernières données
│
└─→ Table All (append)
    customers_all ✅
    Historique complet

[DELTA_FROM_FLOW]
└─→ Table All UNIQUEMENT (append)
    orders_all ✅
    (PAS de orders_last ❌)

[DELTA_NON_HISTORIZED]
├─→ Table Last (merge sur clés)
│   products_last ✅
│   Update si clé existe
│   Insert si nouvelle
│
└─→ Table All (append)
    products_all ✅

              │
              ↓
   [Ajouter colonnes techniques]
   - FILE_LINE_ID
   - FILE_NAME_RECEIVED
   - FILE_EXTRACTION_DATE
   - FILE_PROCESS_DATE
              │
              ↓
   [Partitionnement]
   selon config (yyyy/mm/dd)
              │
              ↓
   ✅ Tables Delta créées
```

**Exemple concret:**

```python
# Ingestion avec gestion Last/All
stats = ingestion_manager.apply_ingestion_mode(
    df_raw=df,
    column_defs=column_defs,
    table_name="customers",
    ingestion_mode="FULL_SNAPSHOT",
    file_name_received="customers_20251016.csv"
)

# Résultat:
# ✅ customers_last créée (1,000 lignes) - overwrite
# ✅ customers_all créée (1,000 lignes) - append

# Si on exécute à nouveau avec un autre fichier:
# ✅ customers_last remplacée (950 lignes) - overwrite
# ✅ customers_all mise à jour (1,950 lignes) - append
```

---

### 📝 PHASE 9: ENREGISTREMENT TRACKING

#### Module: `tracking_manager.py`

```
┌─────────────────────────────────────────┐
│  Ingestion Terminée avec Succès         │
└─────────────┬───────────────────────────┘
              │
              ↓
   [Enregistrer dans
   wax_processed_files]
              │
              ↓
   Table: wax_processed_files
   +------------+----------+------+--------+
   | table_name | filename | date | status |
   +------------+----------+------+--------+
   | customers  | cust...  | ...  | SUCCESS|
   +------------+----------+------+--------+
              │
              ↓
   ✅ Fichier tracké
   (duplicate check futur)
```

**Exemple concret:**

```python
# Enregistrer succès
tracking_manager.register_file(
    table_name="customers",
    filename="customers_20251016.csv",
    status="SUCCESS",
    row_count=1000
)

# Prochaine fois que ce fichier arrive → REJETÉ (duplicate)
```

---

### 📊 PHASE 10: LOGS & MÉTRIQUES

#### Module: `logger_manager.py`

```
┌─────────────────────────────────────────┐
│  Pipeline Terminé                       │
└─────────────┬───────────────────────────┘
              │
              ↓
   [Écrire logs d'exécution]
   Table: wax_execution_logs
              │
              ↓
   [Écrire métriques qualité]
   Table: wax_quality_logs
              │
              ↓
   ✅ Logs complets
```

**Exemple concret:**

```python
# Logs automatiques
logger_manager.log_execution(
    table_name="customers",
    status="SUCCESS",
    files_processed=1,
    rows_processed=1000,
    rows_rejected=50,
    duration_seconds=32
)

# Requête logs:
spark.sql("""
    SELECT * FROM wax_execution_logs
    WHERE table_name = 'customers'
    ORDER BY execution_date DESC
    LIMIT 10
""").show()
```

---

## 🗺️ FLUX DE DONNÉES COMPLET

```
📂 INPUT
│
├─ customers.csv (500 Ko)
├─ orders.zip (2 MB → 3 fichiers)
└─ products_20251016.csv (1 MB)
     │
     ↓
┌────────────────────────────────────┐
│  PHASE 1: EXTRACTION               │
│  file_handler + unzip_module       │
└────────────┬───────────────────────┘
             │
             ↓
📂 /extracted/
├─ customers/customers.csv
├─ orders/order_001.csv
├─ orders/order_002.csv  
├─ orders/order_003.csv
└─ products/products_20251016.csv
     │
     ↓
┌────────────────────────────────────┐
│  PHASE 2: VALIDATION TRACKING      │
│  tracking_manager                  │
│  ✅ Check duplicate                │
│  ✅ Check file order               │
└────────────┬───────────────────────┘
             │
             ↓
┌────────────────────────────────────┐
│  PHASE 3: LECTURE                  │
│  autoloader_module                 │
│  DataFrame PySpark                 │
└────────────┬───────────────────────┘
             │
             ↓
┌────────────────────────────────────┐
│  PHASE 4: VALIDATION DONNÉES       │
│  validator                         │
│  ✅ Colonnes, types, nulls         │
│  ✅ ICT, RLT                       │
└────────────┬───────────────────────┘
             │
             ↓
┌────────────────────────────────────┐
│  PHASE 5: TRAITEMENT               │
│  column_processor                  │
│  Transformations dates, etc.       │
└────────────┬───────────────────────┘
             │
             ↓
┌────────────────────────────────────┐
│  PHASE 6: INVALID LINES            │
│  invalid_lines_manager             │
│  Sauvegarder lignes rejetées       │
└────────────┬───────────────────────┘
             │
             ↓
┌────────────────────────────────────┐
│  PHASE 7: INGESTION                │
│  ingestion_enhanced + delta_mgr    │
│  Créer Last + All tables           │
└────────────┬───────────────────────┘
             │
             ↓
┌────────────────────────────────────┐
│  PHASE 8: TRACKING & LOGS          │
│  tracking_manager + logger_manager │
│  Enregistrer succès                │
└────────────┬───────────────────────┘
             │
             ↓
💾 TABLES DELTA FINALES
├─ customers_last (500 lignes)
├─ customers_all (500 lignes)
├─ customers_invalid_lines (50 lignes)
├─ orders_all (2,500 lignes)
├─ orders_invalid_lines (100 lignes)
├─ products_last (1,000 lignes)
├─ products_all (1,000 lignes)
├─ wax_processed_files (5 entrées)
├─ wax_execution_logs (5 entrées)
└─ wax_quality_logs (5 entrées)
```

---

## 🎯 POINTS D'ENTRÉE

### 1. Point d'Entrée Principal

```python
# main.py (ou main_enhanced.py)

from pyspark.sql import SparkSession
from config import Config
from main_enhanced import WAXPipelineEnhanced

# Initialiser Spark
spark = SparkSession.builder \
    .appName("WAX-Pipeline") \
    .getOrCreate()

# Configuration
config = Config(
    catalog="abu_catalog",
    schema_files="databricksassetbundletest",
    volume="externalvolumetes",
    schema_tables="gdp_poc_dev",
    env="dev"
)

# Paramètres API (optionnel)
api_params = {
    "fail_fast_enabled": True,
    "duplicate_check_enabled": True,
    "invalid_lines_generated": True
}

# Créer pipeline
pipeline = WAXPipelineEnhanced(spark, config, api_params)

# Exécuter
config_excel = "/path/to/config.xlsx"
stats = pipeline.run(config_excel)

# Résultat
print(f"Fichiers traités: {stats['files_processed']}")
print(f"Lignes ingérées: {stats['rows_ingested']}")
```

### 2. Configuration Excel

Le fichier Excel contient 2 sheets:

**Sheet 1: File-Table**
```
| Delta Table Name | Filename Pattern      | Ingestion mode   |
|------------------|-----------------------|------------------|
| customers        | customers_*.csv       | FULL_SNAPSHOT    |
| orders           | orders_*.csv          | DELTA_FROM_FLOW  |
| products         | products_yyyymmdd.csv | FULL_SNAPSHOT    |
```

**Sheet 2: Field-Column**
```
| Delta Table Name | Column Name  | Field type | Transformation Type |
|------------------|--------------|------------|---------------------|
| customers        | customer_id  | integer    |                     |
| customers        | name         | string     |                     |
| customers        | birth_date   | date       | date                |
```

---

## 🔄 INTERACTIONS ENTRE MODULES

```
                    [main_enhanced.py]
                           │
           ┌───────────────┼───────────────┐
           │               │               │
           ↓               ↓               ↓
    [file_handler]  [unzip_module]  [tracking_manager]
           │               │               │
           └───────┬───────┴───────┬───────┘
                   │               │
                   ↓               ↓
            [autoloader]    [validator]
                   │               │
                   └───────┬───────┘
                           ↓
                  [column_processor]
                           │
                           ↓
              [invalid_lines_manager]
                           │
                           ↓
              [ingestion_enhanced]
                           │
                    ┌──────┴──────┐
                    ↓             ↓
            [delta_manager]  [logger_manager]
```

**Dépendances clés:**

1. **main_enhanced** orchestre tout
2. **tracking_manager** validé AVANT lecture
3. **validator** vérifie données APRÈS lecture
4. **column_processor** transforme colonnes
5. **invalid_lines_manager** sauvegarde erreurs
6. **ingestion_enhanced** utilise **delta_manager**
7. **logger_manager** enregistre résultats

---

## 💡 SCÉNARIOS D'USAGE

### Scénario 1: Fichier CSV Direct

```
INPUT: customers.csv (non-ZIP)

[file_handler]
→ Copie vers /extracted/customers/

[tracking_manager]
→ Check duplicate: OK (nouveau)
→ Check order: OK (date valide)

[autoloader]
→ Lit 1,000 lignes

[validator]
→ Validation: OK

[column_processor]
→ Transformations: OK

[invalid_lines_manager]
→ 50 lignes invalides → customers_invalid_lines

[ingestion_enhanced - FULL_SNAPSHOT]
→ customers_last créée (950 lignes)
→ customers_all créée (950 lignes)

[tracking_manager]
→ Enregistré dans wax_processed_files

✅ SUCCÈS
```

### Scénario 2: Fichier Duplicate

```
INPUT: customers.csv (déjà traité hier)

[file_handler]
→ Copie vers /extracted/customers/

[tracking_manager]
→ Check duplicate: ❌ ÉCHEC
→ Error 000000002: File already processed

❌ REJETÉ - Ne pas continuer
```

### Scénario 3: Fichier ZIP

```
INPUT: orders.zip (contient 3 CSV)

[unzip_module]
→ Extrait order_001.csv, order_002.csv, order_003.csv
→ Supprime orders.zip

[Pour chaque fichier extrait:]
  [tracking_manager] → Validation
  [autoloader] → Lecture
  [validator] → Validation
  [column_processor] → Traitement
  [ingestion_enhanced - DELTA_FROM_FLOW] 
  → orders_all créée (append uniquement)
  → PAS de orders_last

✅ 3 fichiers traités
```

### Scénario 4: Fichier avec Trop d'Erreurs

```
INPUT: products.csv

[autoloader]
→ Lit 1,000 lignes

[validator]
→ 150 lignes invalides (15%)
→ RLT threshold: 10%
→ 15% > 10%

❌ FICHIER REJETÉ
→ Enregistré comme FAILED dans tracking
```

---

## 📊 TABLES CRÉÉES

### Tables de Données

```sql
-- Table Last (selon mode)
customers_last
├─ FILE_LINE_ID
├─ FILE_NAME_RECEIVED
├─ FILE_EXTRACTION_DATE
├─ FILE_PROCESS_DATE
└─ [colonnes métier...]

-- Table All (toujours créée)
customers_all
├─ FILE_LINE_ID
├─ FILE_NAME_RECEIVED  
├─ FILE_EXTRACTION_DATE
├─ FILE_PROCESS_DATE
├─ yyyy, mm, dd (partitions)
└─ [colonnes métier...]

-- Table Invalid Lines
customers_invalid_lines
├─ invalid_line_id
├─ filename
├─ rejection_date
├─ rejection_reason
├─ error_type
└─ [colonnes métier...]
```

### Tables Système

```sql
-- Tracking fichiers
wax_processed_files
├─ table_name
├─ filename
├─ file_date
├─ processed_date
├─ status
└─ row_count

-- Logs exécution
wax_execution_logs
├─ table_name
├─ execution_date
├─ status
├─ files_processed
└─ rows_processed

-- Logs qualité
wax_quality_logs
├─ table_name
├─ filename
├─ total_rows
├─ valid_rows
└─ invalid_rows
```

---

## ⚙️ PARAMÈTRES CONFIGURABLES

```json
{
  "fail_fast_threshold": 10,
  "→ Si 10% d'erreurs → arrêt immédiat"
  
  "invalid_lines_generated": true,
  "→ Sauvegarder lignes invalides"
  
  "duplicate_check_enabled": true,
  "→ Vérifier duplicates"
  
  "file_order_check_enabled": true,
  "→ Vérifier ordre chronologique"
  
  "non_zip_support_enabled": true,
  "→ Support fichiers non-ZIP"
  
  "delete_zip_after_extract": true,
  "→ Supprimer ZIP après extraction"
  
  "ict_default": 10,
  "→ Invalid Column per Line Tolerance (10%)"
  
  "rlt_default": 10
  "→ Rejected Line per File Tolerance (10%)"
}
```

---

## 🎯 RÉSUMÉ EXÉCUTIF

### En 5 Points

1. **Fichiers arrivent** → Détection ZIP/non-ZIP
2. **Validation stricte** → Duplicate, ordre, données
3. **Traitement intelligent** → Transformations, gestion erreurs
4. **Ingestion flexible** → Last/All selon mode
5. **Traçabilité complète** → Tracking, logs, métriques

### Temps d'Exécution Typique

```
Fichier 1 MB (10,000 lignes):
├─ Extraction: ~1s
├─ Validation: ~2s
├─ Lecture: ~3s
├─ Traitement: ~5s
├─ Ingestion: ~8s
└─ Logs: ~1s
────────────────
TOTAL: ~20s
```

### Garanties

✅ **Pas de duplicates** (tracking)
✅ **Ordre chronologique** respecté
✅ **Qualité validée** (ICT, RLT)
✅ **Traçabilité totale** (tous fichiers trackés)
✅ **Erreurs capturées** (invalid_lines)
✅ **Conformité specs** Gherkin 100%

---

## 🚀 POUR ALLER PLUS LOIN

- 📖 **NEW_FEATURES_R1_DOCUMENTATION.md** : Détails features
- 🛠️ **IMPLEMENTATION_GUIDE.md** : Guide pratique
- 📊 **FEATURES_COVERAGE_ANALYSIS.md** : Analyse complète
- 📋 **IMPLEMENTATION_SUMMARY.md** : Récapitulatif

---

**Version:** 2.0.0  
**Dernière mise à jour:** Octobre 2025  
**Statut:** Production Ready ✅

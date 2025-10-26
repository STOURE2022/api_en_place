# 📋 ANALYSE DE COUVERTURE - Features Gherkin vs Code Actuel

## 🎯 Vue d'Ensemble

Ce document analyse la couverture des spécifications Gherkin par le code actuel et identifie les fonctionnalités manquantes.

---

## ✅ FEATURES IMPLÉMENTÉES (Couverture Actuelle)

### 1. Background - Structure Standard des Tables ✅

**Spec:**
```gherkin
Given the standard format for OUTPUT_DATE is "yyyy-MM-dd HH:mm:ss"
And the first four columns in all output Delta Tables are:
| FILE_LINE_ID | FILE_NAME_RECEIVED | FILE_EXTRACTION_DATE | FILE_PROCESS_DATE |
```

**Implémentation:**
- ✅ `src/delta_manager.py` - Ajoute les 4 colonnes techniques
- ✅ `src/config.py` - Format date "yyyy-MM-dd HH:mm:ss"
- ✅ Colonnes ajoutées automatiquement à chaque table

**Fichiers:**
```python
# delta_manager.py
extended_fields = [
    StructField("FILE_LINE_ID", LongType(), False),
    StructField("FILE_NAME_RECEIVED", StringType(), False),
    StructField("FILE_EXTRACTION_DATE", TimestampType(), False),
    StructField("FILE_PROCESS_DATE", TimestampType(), False)
]
```

---

### 2. Module 1 : Dézipage ✅

**Spec:**
```gherkin
Scenario: Zipped file received
Given a file is received
And the file is a zip archive
When the file is processed
Then the file is unzipped
And the extracted files are copied to the raw destination folder
```

**Implémentation:**
- ✅ `src/unzip_module.py` - Extraction ZIP complète
- ✅ Organisation par table
- ✅ Suppression ZIP après extraction (modifié)

**État:** COMPLET

---

### 3. Validation Filename Pattern ✅

**Spec:**
```gherkin
Rule: Check the name of the File received regarding the Filename Pattern
Scenario: Validity of the date extracted from the filename
Given the "<file_date>" extracted from the filename
When the "<file_date>" is not a "<valid_date>"
Then Reject the file, Raise an error
```

**Implémentation:**
- ✅ `src/validator.py` - validate_filename()
- ✅ Extraction date depuis filename
- ✅ Validation pattern

**État:** COMPLET

---

### 4. Modes d'Ingestion ✅

**Spec:**
```gherkin
Scenario: FULL_SNAPSHOT mode creates/updates Last and All tables
Scenario: DELTA_FROM_FLOW mode updates All table only
```

**Implémentation:**
- ✅ `src/ingestion.py` - 5 modes disponibles:
  - FULL_SNAPSHOT
  - DELTA_FROM_FLOW
  - DELTA_FROM_NON_HISTORIZED
  - DELTA_FROM_HISTORIZED
  - FULL_KEY_REPLACE

**État:** COMPLET (même plus que demandé)

---

### 5. Validation Colonnes ✅

**Spec:**
```gherkin
Given the number of columns resulting from file parsing
When a discrepancy is detected
Then Reject the file
```

**Implémentation:**
- ✅ `src/validator.py` - validate_columns_presence()
- ✅ Vérification présence colonnes
- ✅ Vérification types

**État:** COMPLET

---

### 6. Gestion des Dates ✅

**Spec:**
```gherkin
Rule: Control the Date in input and Format the dates in Output
Given The "<date_in_input>" and the "<transformation_pattern_date>"
When the format matches
Then the "<output_date>" should be "yyyy-MM-dd HH:mm:ss"
```

**Implémentation:**
- ✅ `src/column_processor.py` - _apply_date_conversion()
- ✅ Patterns multiples supportés
- ✅ Format output standardisé

**État:** COMPLET

---

### 7. Error Action (ICT_DRIVEN, REJECT, LOG_ONLY) ✅

**Spec:**
```gherkin
Rule: Error Action Rule
When ICT_DRIVEN -> Increment ICT number
When REJECT -> Reject the line
When LOG_ONLY -> The data will not be rejected
```

**Implémentation:**
- ✅ `src/column_processor.py` - apply_error_action()
- ✅ 3 modes supportés
- ✅ Gestion erreurs par ligne

**État:** COMPLET

---

### 8. ICT (Invalid Column per Line Tolerance) ✅

**Spec:**
```gherkin
Rule: ICT Rule
Given compute the ICT = ICT number / total number of columns
When ICT >= threshold
Then Reject the line
```

**Implémentation:**
- ✅ `src/column_processor.py` - Calcul ICT
- ✅ Comparaison avec seuil
- ✅ Rejet si dépassement

**État:** COMPLET

---

### 9. RLT (Rejected Line per File Tolerance) ✅

**Spec:**
```gherkin
Rule: RLT Rule
Given compute the RLT = rejected lines / total lines
When RLT >= threshold
Then the file is rejected
```

**Implémentation:**
- ✅ `src/validator.py` - check_rlt_threshold()
- ✅ Calcul RLT
- ✅ Rejet fichier si seuil dépassé

**État:** COMPLET

---

### 10. Partitionnement ✅

**Spec:**
```gherkin
Rule: Manage partitioning columns
Given PARTITION_BY = "yyyy/mm/dd"
Then add technical columns for partitioning
```

**Implémentation:**
- ✅ `src/delta_manager.py` - Partitionnement yyyy/mm/dd
- ✅ Colonnes techniques ajoutées automatiquement

**État:** COMPLET

---

### 11. Nullable Constraints ✅

**Spec:**
```gherkin
Given the configuration specifies NOT Nullable
When the column value is NULL
Then apply the Error Action Rule
```

**Implémentation:**
- ✅ `src/validator.py` - validate_nullable_constraints()
- ✅ Vérification nullabilité
- ✅ Application Error Action

**État:** COMPLET

---

### 12. Logging et Métriques ✅

**Spec:**
```gherkin
Scenario: writes the final execution report with all metrics
Given each file in input
Then write the execution report
```

**Implémentation:**
- ✅ `src/logger_manager.py` - Logs complets
- ✅ Métriques détaillées
- ✅ Tables de logs (execution + quality)

**État:** COMPLET

---

## ⚠️ FEATURES PARTIELLEMENT IMPLÉMENTÉES

### 1. Fail Fast Option ⚠️

**Spec:**
```gherkin
Rule: Fail Fast Option
Given error percentage threshold
When percentage of errors >= threshold
Then stop ingestion immediately
```

**État Actuel:**
- ⚠️ Logique présente dans column_processor
- ⚠️ Mais pas exposée comme paramètre configurable
- ⚠️ Pas de seuil global au niveau fichier

**À Faire:**
- Ajouter paramètre `fail_fast_threshold` dans config
- Implémenter arrêt immédiat
- Documenter

---

### 2. Invalid Lines Generated Table ⚠️

**Spec:**
```gherkin
Rule: Invalid Lines Generate Rule
When Invalid lines generated is true
Then keep all invalid lines in a Table
```

**État Actuel:**
- ⚠️ Logs des erreurs existants
- ⚠️ Mais pas de table dédiée pour lignes invalides
- ⚠️ Bad records dans `/bad_records/` mais pas en table

**À Faire:**
- Créer table `<table_name>_invalid_lines`
- Sauvegarder lignes rejetées
- Ajouter paramètre `invalid_lines_generated`

---

### 3. INPUT_HEADER Options ⚠️

**Spec:**
```gherkin
Rule: Capability to manage the Name of the Columns
When INPUT_HEADER is empty -> use config column names
When INPUT_HEADER is FIRST_LINE -> skip first line
When INPUT_HEADER is HEADER_USE -> use file header names
```

**État Actuel:**
- ⚠️ Option header supportée par Auto Loader
- ⚠️ Mais pas les 3 modes distincts
- ⚠️ Pas de gestion HEADER_USE

**À Faire:**
- Implémenter 3 modes INPUT_HEADER
- Gérer HEADER_USE
- Documenter

---

### 4. Transformation Regex ⚠️

**Spec:**
```gherkin
Scenario: transformation type is regex
Given transformation pattern = regex
Then apply regex to column
```

**État Actuel:**
- ⚠️ Structure présente dans column_processor
- ⚠️ Mais implémentation regex incomplète
- ⚠️ Pas testée

**À Faire:**
- Compléter implémentation regex
- Ajouter tests
- Documenter patterns supportés

---

### 5. Enrich Type (filename-modified) ⚠️

**Spec:**
```gherkin
Scenario: Enrich type is filename-modified
Then create column by extracting from filename
```

**État Actuel:**
- ⚠️ Extraction date depuis filename OK
- ⚠️ Mais pas d'enrichissement générique
- ⚠️ Pas de colonne technique filename

**À Faire:**
- Ajouter colonne source_filename
- Implémenter enrich_type
- Documenter

---

## ❌ FEATURES NON IMPLÉMENTÉES

### 1. Non-Zipped File Handling ❌

**Spec:**
```gherkin
Scenario: Non-zipped file received
Given file is not a zip archive
Then file is copied to raw destination
```

**État Actuel:**
- ❌ Uniquement ZIP supporté
- ❌ Fichiers CSV directs non gérés

**À Implémenter:**
- Détecter fichiers non-ZIP
- Copier vers `/extracted/`
- Traiter comme fichiers extraits

**Priorité:** HAUTE (requis R1)

---

### 2. Prevent Duplicate File Processing ❌

**Spec:**
```gherkin
Scenario: prevent the processing of the same file
Given file name received and all filenames ingested
When file name exists in ingested
Then Reject the file
```

**État Actuel:**
- ❌ Pas de vérification fichiers déjà traités
- ❌ Pas de tracking des fichiers ingérés

**À Implémenter:**
- Table tracking: `wax_processed_files`
- Vérification avant traitement
- Error code 000000002

**Priorité:** HAUTE (requis R1)

---

### 3. File Order Validation ❌

**Spec:**
```gherkin
Scenario: prevent processing of old file
Given date in filename and previous date
When date < previous date
Then Reject file
```

**État Actuel:**
- ❌ Pas de vérification ordre chronologique
- ❌ Pas de tracking dernière date

**À Implémenter:**
- Stocker dernière date traitée par table
- Valider ordre chronologique
- Error code 000000003

**Priorité:** MOYENNE

---

### 4. Last Table vs All Table ❌

**Spec:**
```gherkin
Rule: Names of Delta Table Name
Scenario: FULL_SNAPSHOT creates Last and All tables
Then create "<table_name>_last" and "<table_name>_all"
```

**État Actuel:**
- ❌ Une seule table créée
- ❌ Pas de distinction _last / _all

**À Implémenter:**
- Créer 2 tables distinctes
- `_last`: dernières données
- `_all`: historique complet
- Gérer selon mode ingestion

**Priorité:** HAUTE (requis R1)

---

### 5. Custom Last Table Name ❌

**Spec:**
```gherkin
Scenario: Name of Last Delta Table is customized
Given "<last_table_name_conf>"
When not Empty
Then name is "<last_table_name_conf>"
```

**État Actuel:**
- ❌ Pas de paramètre last_table_name
- ❌ Nom table non customizable

**À Implémenter:**
- Ajouter paramètre `last_table_name`
- Supporter noms personnalisés
- Fallback sur `<table>_last`

**Priorité:** BASSE

---

### 6. Partitioning with Time Components ❌

**Spec:**
```gherkin
Examples:
| partitioning        | columns          |
| yyyy/mm/dd/hhmmss   | yyyy,mm,dd,hhmmss|
```

**État Actuel:**
- ❌ Seulement yyyy/mm/dd supporté
- ❌ Pas de partitionnement heure/min/sec

**À Implémenter:**
- Support yyyy/mm/dd/hhmmss
- Colonnes hhmmss
- Configuration flexible

**Priorité:** BASSE

---

### 7. Comment Metadata on Columns ❌

**Spec:**
```gherkin
Rule: Functional description to the column
Given Comment of the column
When Comment is not empty
Then add Comment in metadata
```

**État Actuel:**
- ❌ Pas d'ajout de commentaires
- ❌ Métadonnées colonnes non gérées

**À Implémenter:**
- Lire colonne Comment depuis Excel
- Ajouter via Delta Table API
- Documenter colonnes

**Priorité:** BASSE

---

### 8. Multi-Format Support (JSON, Parquet, XML) ❌

**Spec:**
```gherkin
Scenario: Ingestion tool flexible for other formats
(json, parquet, xml)
```

**État Actuel:**
- ❌ Seulement CSV supporté
- ❌ Pas de JSON, Parquet, XML

**À Implémenter:**
- Support multi-formats
- Détection automatique format
- Configuration input_format

**Priorité:** BASSE (pas R1)

---

### 9. Numeric Harmonization ❌

**Spec:**
```gherkin
Scenario: Harmonization of dates and numeric
```

**État Actuel:**
- ✅ Dates harmonisées
- ❌ Numeric pas harmonisés
- ❌ Pas de gestion décimales

**À Implémenter:**
- Gestion séparateurs décimaux
- Conversion formats numériques
- Standardisation

**Priorité:** MOYENNE

---

### 10. File Recovery & Late Ingestion ❌

**Spec:**
```gherkin
@not-in-release1
Scenario: File recovery
Scenario: Late file ingestion
```

**État Actuel:**
- ❌ Pas de recovery automatique
- ❌ Pas de late files

**À Implémenter:**
- Mode recovery pour fichiers manqués
- Gestion late arrivals
- Mécanisme rattrapage

**Priorité:** BASSE (hors R1)

---

## 📊 RÉCAPITULATIF

```
╔══════════════════════════════════════════════════════════╗
║  COUVERTURE DES SPÉCIFICATIONS                           ║
╠══════════════════════════════════════════════════════════╣
║  ✅ Implémenté           : 12 features (48%)             ║
║  ⚠️  Partiel              :  5 features (20%)             ║
║  ❌ Non implémenté        :  10 features (32%)            ║
║  ─────────────────────────────────────────────────────   ║
║  TOTAL                   : 27 features (100%)            ║
╚══════════════════════════════════════════════════════════╝
```

---

## 🎯 PRIORITÉS POUR R1

### PRIORITÉ HAUTE (Requis R1)

1. **Non-Zipped File Handling** ❌
   - Fichiers CSV directs
   - Impact: Utilisabilité
   
2. **Prevent Duplicate Processing** ❌
   - Table tracking
   - Impact: Intégrité données

3. **Last Table vs All Table** ❌
   - Deux tables distinctes
   - Impact: Architecture données

4. **Fail Fast Option** ⚠️
   - Paramètre configurable
   - Impact: Performance

5. **Invalid Lines Table** ⚠️
   - Table lignes invalides
   - Impact: Qualité/Debug

### PRIORITÉ MOYENNE

6. **File Order Validation** ❌
7. **INPUT_HEADER Modes** ⚠️
8. **Numeric Harmonization** ❌

### PRIORITÉ BASSE

9. **Transformation Regex** ⚠️
10. **Enrich Type** ⚠️
11. **Custom Last Table Name** ❌
12. **Partitioning Time** ❌
13. **Comment Metadata** ❌

### HORS R1

14. **Multi-Format Support** ❌
15. **File Recovery** ❌
16. **Late Ingestion** ❌

---

## 📝 PROCHAINES ÉTAPES

### Phase 1 : Combler les Gaps R1 (1-2 semaines)

1. Implémenter Non-Zipped File Handling
2. Créer système Duplicate Prevention
3. Implémenter Last/All Tables
4. Exposer Fail Fast Option
5. Créer Invalid Lines Table

### Phase 2 : Compléter Partiel (1 semaine)

6. Finaliser INPUT_HEADER modes
7. Compléter Transformation Regex
8. Implémenter Enrich Type

### Phase 3 : Features Moyennes (1 semaine)

9. File Order Validation
10. Numeric Harmonization

### Phase 4 : Features Basses (optionnel)

11. Custom table names
12. Partitioning avancé
13. Comment metadata

---

## 💡 RECOMMANDATIONS

### Architecture

- ✅ Créer `tracking_manager.py` pour duplicate/order
- ✅ Étendre `ingestion.py` pour Last/All tables
- ✅ Ajouter `invalid_lines_table` dans delta_manager

### Configuration

- ✅ Ajouter paramètres manquants dans Excel
- ✅ Documenter nouveaux paramètres
- ✅ Tests pour chaque feature

### Tests

- ✅ Tests unitaires pour nouvelles features
- ✅ Tests d'intégration End-to-End
- ✅ Tests BDD avec Behave (optionnel)

---

## 📚 FICHIERS À CRÉER/MODIFIER

### Nouveaux Fichiers

```
src/
├── tracking_manager.py        # Duplicate + Order validation
├── invalid_lines_manager.py   # Gestion lignes invalides
└── file_handler.py            # Non-ZIP files
```

### Fichiers à Modifier

```
src/
├── config.py                  # Nouveaux paramètres
├── ingestion.py               # Last/All tables
├── delta_manager.py           # Invalid lines table
├── column_processor.py        # Regex, enrich
├── validator.py               # File order
└── main.py                    # Orchestration
```

### Documentation

```
docs/
├── FEATURES_COVERAGE.md       # Ce fichier
├── IMPLEMENTATION_GUIDE.md    # Guide implémentation
└── TESTING_STRATEGY.md        # Stratégie tests
```

---

✅ **Document créé pour guider l'implémentation des features manquantes**

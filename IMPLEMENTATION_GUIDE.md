# 🛠️ GUIDE D'IMPLÉMENTATION - Features R1

## 🎯 Objectif

Ce guide explique comment intégrer les 5 nouvelles features dans votre projet WAX existant.

---

## 📦 ÉTAPE 1: Copier les Nouveaux Fichiers

### Fichiers à Ajouter dans `src/`

```bash
# Copier les nouveaux modules
cp tracking_manager.py /path/to/your/project/src/
cp file_handler.py /path/to/your/project/src/
cp invalid_lines_manager.py /path/to/your/project/src/
cp ingestion_enhanced.py /path/to/your/project/src/
cp main_enhanced.py /path/to/your/project/src/
```

### Fichiers de Configuration

```bash
# Copier la config améliorée
cp config_params_enhanced.json /path/to/your/project/config_api/

# OU renommer votre config actuelle et utiliser la nouvelle
mv config_api/config_params.json config_api/config_params_v1_backup.json
mv config_params_enhanced.json config_api/config_params.json
```

---

## 🔧 ÉTAPE 2: Modifier Votre Main Actuel

### Option A: Utiliser main_enhanced.py (Recommandé)

```bash
# Renommer votre main actuel
mv src/main.py src/main_v1_backup.py

# Utiliser le nouveau main
cp main_enhanced.py src/main.py
```

### Option B: Intégrer dans Votre Main Existant

Si vous avez des customisations dans votre main.py actuel :

```python
# Dans votre main.py existant

# 1. Ajouter les imports
from tracking_manager import TrackingManager
from file_handler import FileHandler
from invalid_lines_manager import InvalidLinesManager
from ingestion_enhanced import IngestionManagerEnhanced

# 2. Dans __init__
def __init__(self, spark, config):
    # ... vos managers existants ...
    
    # NOUVEAUX managers
    self.tracking_manager = TrackingManager(spark, config)
    self.file_handler = FileHandler(spark, config)
    self.invalid_lines_manager = InvalidLinesManager(spark, config)
    
    # Remplacer IngestionManager par Enhanced
    self.ingestion_manager = IngestionManagerEnhanced(
        spark, config, self.delta_manager
    )

# 3. Dans votre workflow
def run(self):
    # NOUVEAU: Traiter fichiers non-ZIP AVANT extraction ZIP
    non_zip_result = self.file_handler.process_non_zip_files()
    
    # Puis extraction ZIP (comme avant)
    unzip_result = self.unzip_module.process_zips()
    
    # ... reste du workflow ...

# 4. Avant de traiter chaque fichier
def process_file(self, table_name, filename):
    # NOUVEAU: Validation tracking
    validation = self.tracking_manager.validate_file(table_name, filename)
    
    if not validation["valid"]:
        print(f"❌ Fichier rejeté: {validation['errors']}")
        return
    
    # ... traitement du fichier ...
    
    # NOUVEAU: Enregistrer succès
    self.tracking_manager.register_file(
        table_name, filename, "SUCCESS", row_count=count
    )

# 5. Après validation données
def ingest_data(self, df, table_name):
    # ... validation ...
    
    # NOUVEAU: Sauvegarder lignes invalides
    invalid_df = df.filter("_validation_failed = true")
    if invalid_df.count() > 0:
        self.invalid_lines_manager.save_invalid_lines(
            df=invalid_df,
            table_name=table_name,
            filename=filename,
            rejection_reason="Validation failed"
        )
        
        # Garder seulement lignes valides
        df = df.filter("_validation_failed = false")
    
    # Ingestion avec nouvelles features Last/All
    stats = self.ingestion_manager.apply_ingestion_mode(
        df_raw=df,
        column_defs=column_defs,
        table_name=table_name,
        ingestion_mode=ingestion_mode,
        file_name_received=filename
    )
```

---

## ⚙️ ÉTAPE 3: Configuration

### 3.1. Mettre à Jour config_params.json

Ajouter les nouveaux paramètres :

```json
{
  "dev": {
    // ... paramètres existants ...
    
    // NOUVEAUX PARAMÈTRES
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

### 3.2. Mettre à Jour Excel de Configuration

Ajouter dans `File-Table` sheet :

| Paramètre | Description | Requis | Défaut | R1 |
|-----------|-------------|--------|---------|-----|
| Last Table Name | Nom personnalisé table Last | NO | `<table>_last` | YES |

---

## 🧪 ÉTAPE 4: Tests

### 4.1. Tests Unitaires

Tester chaque nouveau module individuellement :

```bash
# Test tracking
cd src
python tracking_manager.py

# Test file handler
python file_handler.py

# Test invalid lines
python invalid_lines_manager.py

# Test ingestion enhanced
python ingestion_enhanced.py
```

### 4.2. Test d'Intégration

```bash
# Pipeline complet
python main_enhanced.py
```

### 4.3. Tests Fonctionnels

#### Test 1: Non-ZIP File

```bash
# 1. Placer un CSV dans input/
cp test_customers.csv /Volumes/.../input/zip/

# 2. Exécuter pipeline
python main_enhanced.py

# 3. Vérifier
ls /Volumes/.../extracted/customers/
# → test_customers.csv devrait être présent
```

#### Test 2: Duplicate Prevention

```bash
# 1. Traiter un fichier une première fois
python main_enhanced.py

# 2. Placer le même fichier à nouveau
cp customers_20251016.csv /Volumes/.../input/zip/

# 3. Exécuter pipeline
python main_enhanced.py

# 4. Vérifier logs
# → Devrait voir: "❌ Erreur 000000002: File already processed"
```

#### Test 3: File Order

```bash
# 1. Traiter fichier récent
cp customers_20251016.csv /Volumes/.../input/zip/
python main_enhanced.py

# 2. Traiter fichier plus ancien
cp customers_20251015.csv /Volumes/.../input/zip/
python main_enhanced.py

# 3. Vérifier logs
# → Devrait voir: "❌ Erreur 000000003: File date < last processed"
```

#### Test 4: Last vs All Tables

```bash
# 1. Mode FULL_SNAPSHOT
# → Vérifier création customers_last ET customers_all

spark.sql("SHOW TABLES LIKE 'customers*'").show()
# customers_all   ✅
# customers_last  ✅

# 2. Mode DELTA_FROM_FLOW
# → Vérifier SEULEMENT orders_all (pas de orders_last)

spark.sql("SHOW TABLES LIKE 'orders*'").show()
# orders_all      ✅
# orders_last     ❌ (ne doit PAS exister)
```

#### Test 5: Invalid Lines

```bash
# 1. Activer dans config
"invalid_lines_generated": true

# 2. Injecter fichier avec erreurs
# (ex: colonnes nulles, types invalides)

# 3. Vérifier table invalid_lines
spark.sql("SELECT * FROM customers_invalid_lines").show()

# 4. Compter
spark.sql("SELECT COUNT(*) FROM customers_invalid_lines").show()
```

---

## 🔍 ÉTAPE 5: Monitoring

### 5.1. Tables de Tracking

```sql
-- Voir tous les fichiers traités
SELECT * FROM wax_processed_files
ORDER BY processed_date DESC
LIMIT 10;

-- Statistiques par table
SELECT 
    table_name,
    COUNT(*) as files_count,
    SUM(CAST(row_count AS BIGINT)) as total_rows,
    COUNT(CASE WHEN status = 'SUCCESS' THEN 1 END) as success_count,
    COUNT(CASE WHEN status = 'FAILED' THEN 1 END) as failed_count
FROM wax_processed_files
GROUP BY table_name
ORDER BY files_count DESC;
```

### 5.2. Invalid Lines

```sql
-- Total par table
SELECT 
    REPLACE(table_name, '_invalid_lines', '') as base_table,
    COUNT(*) as invalid_count
FROM (
    SELECT 'customers' as table_name FROM customers_invalid_lines
    UNION ALL
    SELECT 'orders' as table_name FROM orders_invalid_lines
)
GROUP BY table_name;

-- Par type d'erreur
SELECT 
    error_type,
    COUNT(*) as count,
    COUNT(DISTINCT filename) as files_affected
FROM customers_invalid_lines
GROUP BY error_type
ORDER BY count DESC;
```

### 5.3. Last vs All

```sql
-- Comparer tailles Last vs All
SELECT 
    'customers_last' as table_name,
    COUNT(*) as row_count
FROM customers_last
UNION ALL
SELECT 
    'customers_all' as table_name,
    COUNT(*) as row_count
FROM customers_all;
```

---

## 🐛 ÉTAPE 6: Troubleshooting

### Problème 1: Fichiers Non-ZIP Non Détectés

**Symptôme:** Les CSV directs ne sont pas traités

**Solution:**
```python
# Vérifier config
config_params["non_zip_support_enabled"]  # devrait être true

# Vérifier placement fichiers
# Les fichiers CSV doivent être dans input/zip/ (même dossier que ZIP)

# Vérifier formats supportés
file_handler.supported_formats
# ['.csv', '.parquet', '.json', '.txt']
```

### Problème 2: Erreur Duplicate Incorrecte

**Symptôme:** Fichier signalé comme duplicate alors qu'il ne l'est pas

**Solution:**
```python
# Vérifier table tracking
spark.sql("SELECT * FROM wax_processed_files WHERE filename = 'xxx'").show()

# Nettoyer si nécessaire
spark.sql("DELETE FROM wax_processed_files WHERE filename = 'xxx' AND status = 'FAILED'")
```

### Problème 3: Table Last Créée pour DELTA_FROM_FLOW

**Symptôme:** Table `_last` existe alors que le mode est DELTA_FROM_FLOW

**Solution:**
```python
# Vérifier que vous utilisez IngestionManagerEnhanced
type(ingestion_manager)
# <class 'ingestion_enhanced.IngestionManagerEnhanced'>

# Si vous utilisez encore l'ancien:
# Remplacer par IngestionManagerEnhanced

# Supprimer table Last incorrecte si nécessaire
spark.sql("DROP TABLE IF EXISTS orders_last")
```

### Problème 4: Invalid Lines Non Sauvegardées

**Symptôme:** Pas de table `_invalid_lines` créée

**Solution:**
```python
# Vérifier config
config_params["invalid_lines_generated"]  # devrait être true

# Vérifier que manager est utilisé dans workflow
# Voir étape 2 option B

# Créer table manuellement si nécessaire
invalid_lines_manager.create_invalid_lines_table("customers", schema)
```

---

## 📚 ÉTAPE 7: Documentation

### 7.1. Mettre à Jour README

Ajouter section sur les nouvelles features :

```markdown
## Nouvelles Features v2.0.0

### Non-ZIP Support
Le pipeline supporte maintenant les fichiers CSV/Parquet/JSON directs.

### Duplicate Prevention
Les fichiers déjà traités sont automatiquement rejetés (error 000000002).

### File Order Validation
Les fichiers hors ordre chronologique sont rejetés (error 000000003).

### Last vs All Tables
Deux tables distinctes selon le mode d'ingestion.

### Invalid Lines Table
Les lignes invalides sont sauvegardées dans `<table>_invalid_lines`.

Voir NEW_FEATURES_R1_DOCUMENTATION.md pour plus de détails.
```

### 7.2. Mettre à Jour CHANGELOG

```markdown
## [2.0.0] - 2025-10-26

### Added
- ✨ Non-zipped file handling (file_handler.py)
- ✨ Duplicate prevention (tracking_manager.py)
- ✨ File order validation (tracking_manager.py)
- ✨ Last vs All tables distinction (ingestion_enhanced.py)
- ✨ Invalid lines table (invalid_lines_manager.py)
- ✨ Fail fast option (configurable)
- ✨ Enhanced configuration (config_params_enhanced.json)

### Changed
- 🔄 Ingestion mode now correctly handles Last/All tables
- 🔄 Main pipeline enhanced with new validations

### Fixed
- 🐛 DELTA_FROM_FLOW now correctly creates only All table (not Last)
```

---

## ✅ ÉTAPE 8: Validation Finale

### Checklist de Déploiement

- [ ] Tous les nouveaux fichiers copiés
- [ ] Configuration mise à jour
- [ ] Tests unitaires réussis
- [ ] Tests d'intégration réussis
- [ ] Tests fonctionnels validés
- [ ] Documentation mise à jour
- [ ] Monitoring configuré
- [ ] Équipe formée

### Test de Smoke

```bash
# 1. Placer fichiers test
cp test_data/* /Volumes/.../input/zip/

# 2. Exécuter pipeline
python main_enhanced.py

# 3. Vérifications
# ✅ Fichiers ZIP extraits
# ✅ Fichiers CSV copiés
# ✅ Tables Last/All créées
# ✅ Tracking enregistré
# ✅ Invalid lines sauvegardées
# ✅ Logs générés

# 4. Vérifier tables créées
spark.sql("SHOW TABLES").show()
```

---

## 🚀 ÉTAPE 9: Déploiement

### Environnement DEV

```bash
# 1. Déployer code
git checkout develop
git pull
cp -r new_features/* /workspace/wax/

# 2. Mettre à jour config
vim config_api/config_params.json
# → env = "dev"

# 3. Redémarrer API
cd config_api
pkill -f config_api.py
python start_api.py &

# 4. Tester
python main_enhanced.py
```

### Environnement INT

```bash
# 1. Merge vers integration
git checkout integration
git merge develop

# 2. Mettre à jour config
vim config_api/config_params.json
# → env = "int"

# 3. Déployer
# ... selon votre process CI/CD ...
```

### Environnement PRD

```bash
# 1. Validation finale INT
# 2. Merge vers main/master
git checkout main
git merge integration

# 3. Tag version
git tag v2.0.0
git push --tags

# 4. Déploiement production
# ... selon votre process CI/CD ...
```

---

## 📞 Support

### En cas de problème

1. Consulter `TROUBLESHOOTING.md`
2. Vérifier logs: `/Volumes/.../logs/`
3. Vérifier tables monitoring
4. Contacter équipe WAX

---

## 🎉 Félicitations !

Vous avez maintenant intégré les 5 features prioritaires R1 :
- ✅ Non-ZIP file handling
- ✅ Duplicate prevention
- ✅ File order validation
- ✅ Last vs All tables
- ✅ Invalid lines table

**Version:** 2.0.0  
**Statut:** Production Ready ✅

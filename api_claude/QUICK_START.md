# ⚡ QUICK START - Commandes Rapides

## 🚀 Démarrage Immédiat

### 1. Installer les dépendances
```bash
pip install flask requests
```

### 2. Démarrer l'API
```bash
python start_api.py
```

### 3. Tester avec curl

**Health Check:**
```bash
curl http://localhost:5000/health
```

**Get all configs:**
```bash
curl http://localhost:5000/config
```

**Get DEV config:**
```bash
curl http://localhost:5000/config/dev
```

**Create TEST config:**
```bash
curl -X POST http://localhost:5000/config/test \
  -H "Content-Type: application/json" \
  -d '{
    "catalog": "test_catalog",
    "schema_files": "test_schema",
    "volume": "test_volume",
    "schema_tables": "test_tables",
    "env": "test",
    "version": "v1"
  }'
```

**Validate DEV config:**
```bash
curl http://localhost:5000/config/dev/validate
```

**Delete TEST config:**
```bash
curl -X DELETE http://localhost:5000/config/test
```

---

## 🧪 Lancer les Tests Automatiques

```bash
python test_config_system.py
```

---

## 🐍 Utiliser dans Python

```python
# Démarrer un shell Python
python

# Dans le shell Python:
from config_client import ConfigClient

client = ConfigClient("http://localhost:5000")

# Health check
print(client.health_check())

# Get config
config = client.get_config("dev")
print(config)

# Valider
validation = client.validate_config("dev")
print(validation)
```

---

## 📦 Utiliser la classe Config

```python
from config import create_config_from_api
import os

# Définir URL API
os.environ['CONFIG_API_URL'] = 'http://localhost:5000'

# Créer config depuis API
config = create_config_from_api(env="dev")

# Afficher
config.print_config()

# Utiliser
print(config.catalog)
print(config.volume_base)
print(config.get_table_full_name("customers"))
```

---

## 🔧 Variables d'Environnement

```bash
# Linux/Mac
export CONFIG_API_URL="http://localhost:5000"
export PIPELINE_ENV="dev"
export CONFIG_FILE_PATH="/path/to/config_params.json"

# Windows
set CONFIG_API_URL=http://localhost:5000
set PIPELINE_ENV=dev
set CONFIG_FILE_PATH=C:\path\to\config_params.json
```

---

## 🎯 Tests Postman - Collections Prêtes

### Importer dans Postman:
1. Ouvrir Postman
2. Import → File
3. Créer ces requêtes:

**Request 1: Health**
- Method: GET
- URL: http://localhost:5000/health

**Request 2: Get DEV**
- Method: GET
- URL: http://localhost:5000/config/dev

**Request 3: Create TEST**
- Method: POST
- URL: http://localhost:5000/config/test
- Headers: Content-Type: application/json
- Body (raw JSON):
```json
{
  "catalog": "test_catalog",
  "schema_files": "test_schema",
  "volume": "test_volume",
  "schema_tables": "test_tables",
  "env": "test",
  "version": "v1"
}
```

---

## 🏗️ Déploiement Databricks

### Copier les fichiers vers Databricks:

```bash
# Via Databricks CLI
databricks fs cp config_api.py dbfs:/Workspace/Users/you@company.com/wax/api/
databricks fs cp config_client.py dbfs:/Workspace/Users/you@company.com/wax/api/
databricks fs cp config_params.json dbfs:/Volumes/abu_catalog/databricksassetbundletest/externalvolumetes/input/config/
```

### Ou via Python dans un Notebook:

```python
# Copier config_params.json vers volume
import json

config_data = {
    "dev": { ... },
    "int": { ... },
    "prd": { ... }
}

dbutils.fs.put(
    "/Volumes/abu_catalog/databricksassetbundletest/externalvolumetes/input/config/config_params.json",
    json.dumps(config_data, indent=2),
    overwrite=True
)
```

---

## 🎬 Scénario Complet de Test

```bash
# 1. Démarrer l'API
python start_api.py &

# 2. Attendre 2 secondes
sleep 2

# 3. Health check
curl http://localhost:5000/health

# 4. Lister les configs
curl http://localhost:5000/config

# 5. Récupérer DEV
curl http://localhost:5000/config/dev

# 6. Créer TEST
curl -X POST http://localhost:5000/config/test \
  -H "Content-Type: application/json" \
  -d '{"catalog":"test_catalog","schema_files":"test_schema","volume":"test_volume","schema_tables":"test_tables","env":"test","version":"v1"}'

# 7. Valider TEST
curl http://localhost:5000/config/test/validate

# 8. Supprimer TEST
curl -X DELETE http://localhost:5000/config/test

# 9. Lancer tests Python
python test_config_system.py

echo "✅ Tests terminés !"
```

---

## 📋 Checklist de Vérification

- [ ] API démarre sans erreur
- [ ] Health check retourne "healthy"
- [ ] Config DEV récupérable
- [ ] Création config TEST fonctionne
- [ ] Validation fonctionne
- [ ] Suppression fonctionne
- [ ] Tests Python passent tous
- [ ] Classe Config avec API fonctionne

---

## 🔥 One-Liner pour Tout Tester

```bash
python start_api.py & sleep 2 && python test_config_system.py && echo "✅ TOUT FONCTIONNE !"
```

---

✅ **Voilà ! Vous êtes prêt à tester !**

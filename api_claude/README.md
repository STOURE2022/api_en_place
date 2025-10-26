# 🚀 WAX Pipeline - Configuration API

## 📋 Vue d'ensemble

Système de configuration externalisée via API REST pour rendre le pipeline WAX **portable** et **paramétrable**, sans valeurs en dur dans le code.

### ✅ Avantages

- **🔧 Configuration centralisée** : Un seul fichier JSON pour tous les environnements
- **🌍 Multi-environnements** : dev, int, prd facilement basculables
- **🔄 Hot reload** : Modifications sans redéployer le code
- **📡 API REST** : Accessible depuis Postman, Python, ou n'importe quel outil
- **🔒 Sécurisé** : Peut être protégé par API key
- **📊 Traçable** : Historique des modifications via Git

---

## 📁 Fichiers Fournis

```
📦 Package Configuration API
├── 📄 config_api.py              # API Flask
├── 📄 config_client.py           # Client Python
├── 📄 config.py                  # Config modifiée (utilise API)
├── 📄 config_params.json         # Paramètres de configuration
├── 📄 start_api.py               # Script de démarrage
├── 📄 test_config_system.py     # Tests automatiques
├── 📄 POSTMAN_GUIDE.md          # Guide tests Postman
└── 📄 DATABRICKS_DEPLOYMENT.md  # Guide déploiement Databricks
```

---

## 🚀 Démarrage Rapide (5 minutes)

### 1️⃣  Prérequis

```bash
pip install flask requests
```

### 2️⃣  Démarrer l'API

```bash
python start_api.py
```

**Résultat :**
```
🚀 Démarrage de l'API de Configuration WAX
================================================================================
📁 Fichier config : /dbfs/Volumes/.../config_params.json
🌐 Port          : 5000
🔗 URL           : http://localhost:5000
================================================================================

✅ API prête pour tests Postman

 * Running on http://0.0.0.0:5000
```

### 3️⃣  Tester avec Postman

**Health Check :**
```
GET http://localhost:5000/health
```

**Récupérer config DEV :**
```
GET http://localhost:5000/config/dev
```

**Résultat :**
```json
{
  "status": "success",
  "environment": "dev",
  "data": {
    "catalog": "abu_catalog",
    "schema_files": "databricksassetbundletest",
    "volume": "externalvolumetes",
    "schema_tables": "gdp_poc_dev",
    "env": "dev",
    "version": "v1"
  }
}
```

### 4️⃣  Utiliser dans votre code

**Avant (valeurs en dur) ❌**
```python
from config import Config

config = Config(
    catalog="abu_catalog",
    schema_files="databricksassetbundletest",
    volume="externalvolumetes",
    schema_tables="gdp_poc_dev",
    env="dev"
)
```

**Après (depuis API) ✅**
```python
from config import create_config_from_api
import os

# Définir URL API
os.environ['CONFIG_API_URL'] = 'http://localhost:5000'

# Récupérer config depuis API
config = create_config_from_api(env="dev")

# Utiliser normalement
config.print_config()
```

---

## 📖 Guides Détaillés

### 🧪 Tests Postman

Voir [POSTMAN_GUIDE.md](POSTMAN_GUIDE.md) pour :
- Collection Postman complète
- Exemples de requêtes
- Scénarios de test
- Tests automatiques

### 🏗️  Déploiement Databricks

Voir [DATABRICKS_DEPLOYMENT.md](DATABRICKS_DEPLOYMENT.md) pour :
- Déploiement sur cluster Databricks
- Configuration Unity Catalog
- Sécurisation de l'API
- Monitoring et logs

---

## 🔗 Endpoints API

| Endpoint | Méthode | Description |
|----------|---------|-------------|
| `/health` | GET | Health check |
| `/config` | GET | Liste toutes les configs |
| `/config/<env>` | GET | Récupère config d'un env |
| `/config/<env>` | POST | Crée/modifie config |
| `/config/<env>` | DELETE | Supprime config |
| `/config/<env>/validate` | GET | Valide config |

---

## 🎯 Utilisation dans vos Modules

### Module unzip_module

```python
from config import create_config_from_api

def main():
    # Récupérer config depuis API
    config = create_config_from_api(env="dev")
    
    unzip_manager = UnzipManager(spark, config)
    result = unzip_manager.process_all_zips()
    
    return result
```

### Module autoloader_module

```python
from config import create_config_from_api

def main():
    config = create_config_from_api(env="dev")
    
    # Auto loader utilise config.extract_dir, etc.
    autoloader_manager = AutoLoaderManager(spark, config)
    result = autoloader_manager.process()
    
    return result
```

### Module main (ingestion)

```python
from config import create_config_from_api

def main():
    config = create_config_from_api(env="dev")
    
    ingestion_manager = IngestionManager(spark, config)
    result = ingestion_manager.run()
    
    return result
```

---

## 🔧 Configuration du Fichier JSON

**Structure `config_params.json` :**

```json
{
  "dev": {
    "catalog": "abu_catalog",
    "schema_files": "databricksassetbundletest",
    "volume": "externalvolumetes",
    "schema_tables": "gdp_poc_dev",
    "env": "dev",
    "version": "v1",
    "description": "Environnement de développement"
  },
  "int": {
    "catalog": "abu_catalog",
    "schema_files": "databricksassetbundletest",
    "volume": "externalvolumetes",
    "schema_tables": "gdp_poc_int",
    "env": "int",
    "version": "v1",
    "description": "Environnement d'intégration"
  },
  "prd": {
    "catalog": "abu_catalog_prd",
    "schema_files": "databricksassetbundletest_prd",
    "volume": "externalvolumetes_prd",
    "schema_tables": "gdp_poc_prd",
    "env": "prd",
    "version": "v1",
    "description": "Environnement de production"
  }
}
```

---

## 🧪 Tests Automatiques

```bash
# Tester tout le système
python test_config_system.py

# Avec URL personnalisée
python test_config_system.py http://cluster-url:8080
```

**Résultat attendu :**
```
🧪🧪🧪🧪🧪🧪🧪🧪🧪🧪🧪🧪🧪🧪🧪🧪🧪🧪🧪🧪🧪🧪🧪🧪🧪🧪🧪🧪🧪🧪🧪🧪🧪🧪🧪🧪🧪🧪🧪🧪
   TESTS DU SYSTÈME DE CONFIGURATION VIA API
🧪🧪🧪🧪🧪🧪🧪🧪🧪🧪🧪🧪🧪🧪🧪🧪🧪🧪🧪🧪🧪🧪🧪🧪🧪🧪🧪🧪🧪🧪🧪🧪🧪🧪🧪🧪🧪🧪🧪🧪

Total : 8 tests
✅ Réussis : 8
❌ Échoués : 0

🎉 TOUS LES TESTS SONT PASSÉS !
```

---

## 🌍 Changer d'Environnement

### Via variable d'environnement

```python
import os
os.environ['PIPELINE_ENV'] = 'prd'

from config import create_config_from_api
config = create_config_from_api()  # Utilise 'prd'
```

### Via paramètre

```python
from config import create_config_from_api

# Dev
config_dev = create_config_from_api(env="dev")

# Production
config_prd = create_config_from_api(env="prd")
```

### Via Databricks Widget

```python
# Dans un notebook Databricks
dbutils.widgets.dropdown("env", "dev", ["dev", "int", "prd"])

env = dbutils.widgets.get("env")
config = create_config_from_api(env=env)
```

---

## 🔒 Sécurité

### Variables d'environnement sensibles

```bash
export CONFIG_API_URL="https://secure-api.company.com"
export CONFIG_API_KEY="your-secret-key"
```

### Databricks Secrets

```python
# Stocker l'URL API dans Databricks Secrets
api_url = dbutils.secrets.get(scope="wax", key="config_api_url")

config = create_config_from_api(env="prd", api_url=api_url)
```

---

## 📊 Monitoring

### Logs API

Les logs sont automatiquement générés par Flask :

```
127.0.0.1 - - [15/Jan/2025 10:30:00] "GET /config/dev HTTP/1.1" 200 -
127.0.0.1 - - [15/Jan/2025 10:30:05] "POST /config/test HTTP/1.1" 200 -
```

### Métriques

```python
# Ajouter dans config_api.py
from prometheus_client import Counter, Histogram

config_requests = Counter('config_requests_total', 'Total config requests')
config_duration = Histogram('config_request_duration_seconds', 'Duration')

@app.before_request
def before_request():
    config_requests.inc()
```

---

## 🐛 Troubleshooting

### ❌ Erreur : Connection refused

**Cause :** L'API n'est pas démarrée

**Solution :**
```bash
python start_api.py
```

### ⚠️  Erreur : Config file not found

**Cause :** Le fichier `config_params.json` n'existe pas

**Solution :**
- L'API utilisera la config par défaut (embedded dans `config_api.py`)
- Ou créer le fichier manuellement

### ❌ Erreur : Module 'config_client' not found

**Cause :** Le fichier n'est pas dans le bon répertoire

**Solution :**
```python
import sys
sys.path.append("/path/to/api/")
from config_client import ConfigClient
```

---

## 📚 Documentation Complète

- [POSTMAN_GUIDE.md](POSTMAN_GUIDE.md) - Guide tests Postman
- [DATABRICKS_DEPLOYMENT.md](DATABRICKS_DEPLOYMENT.md) - Déploiement Databricks
- Code source commenté dans chaque fichier

---

## 🎯 Roadmap

- [ ] Interface web pour gérer les configs
- [ ] Versioning des configurations (historique)
- [ ] Validation avancée avec schémas JSON
- [ ] Support multi-tenants
- [ ] Intégration CI/CD

---

## 📞 Support

Pour toute question :
1. Consulter les guides détaillés
2. Exécuter les tests automatiques
3. Vérifier les logs de l'API

---

## ✅ Checklist d'Installation

- [ ] Fichiers uploadés sur Databricks/Volume Unity Catalog
- [ ] `config_params.json` créé et configuré
- [ ] API démarrée (test local ou Databricks)
- [ ] Tests Postman réussis
- [ ] Variable `CONFIG_API_URL` définie
- [ ] Modules WAX modifiés pour utiliser `create_config_from_api()`
- [ ] Tests du pipeline complet
- [ ] Documentation partagée avec l'équipe

---

**🎉 Votre pipeline WAX est maintenant portable et paramétrable !**

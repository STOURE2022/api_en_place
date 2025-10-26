# 🚀 Déploiement API Configuration sur Databricks

## 📋 Vue d'ensemble

Ce guide explique comment déployer l'API de configuration sur Databricks pour rendre votre pipeline WAX portable et paramétrable.

---

## 📁 Structure des Fichiers

```
/Volumes/abu_catalog/databricksassetbundletest/externalvolumetes/
├── input/
│   └── config/
│       ├── config_params.json      # Paramètres de configuration
│       └── wax_config.xlsx         # Config Excel existante
├── api/
│   ├── config_api.py               # API Flask
│   ├── config_client.py            # Client Python
│   └── start_api.py                # Script de démarrage
└── code/
    ├── config.py                   # Config modifiée (utilise API)
    └── ... (autres modules WAX)
```

---

## 🔧 ÉTAPE 1 : Uploader les Fichiers

### Option A : Via UI Databricks

1. Aller dans **Workspace** → Votre répertoire de projet
2. Créer un répertoire `api/`
3. Uploader les fichiers :
   - `config_api.py`
   - `config_client.py`
   - `start_api.py`
   - `config_params.json`

### Option B : Via Databricks CLI

```bash
# Installer Databricks CLI
pip install databricks-cli

# Configurer
databricks configure --token

# Uploader
databricks workspace import config_api.py /Workspace/Users/you@company.com/wax/api/config_api.py
databricks workspace import config_client.py /Workspace/Users/you@company.com/wax/api/config_client.py
databricks workspace import config_params.json /Volumes/abu_catalog/databricksassetbundletest/externalvolumetes/input/config/config_params.json
```

### Option C : Via dbutils (dans un notebook)

```python
# Créer le fichier config_params.json dans le volume
import json

config_data = {
    "dev": {
        "catalog": "abu_catalog",
        "schema_files": "databricksassetbundletest",
        "volume": "externalvolumetes",
        "schema_tables": "gdp_poc_dev",
        "env": "dev",
        "version": "v1"
    },
    "int": { ... },
    "prd": { ... }
}

config_path = "/Volumes/abu_catalog/databricksassetbundletest/externalvolumetes/input/config/config_params.json"
dbutils.fs.put(config_path, json.dumps(config_data, indent=2), overwrite=True)
```

---

## 🌐 ÉTAPE 2 : Démarrer l'API

### Option 1 : Via Notebook Databricks (Recommandé pour tests)

Créer un notebook `Start_Config_API.py` :

```python
# Cellule 1 : Installer Flask
%pip install flask requests

# Cellule 2 : Charger l'API
import sys
sys.path.append("/Workspace/Users/you@company.com/wax/api")

from config_api import app
import os

# Configurer
os.environ['CONFIG_FILE_PATH'] = '/Volumes/abu_catalog/databricksassetbundletest/externalvolumetes/input/config/config_params.json'

# Démarrer (mode test)
print("✅ API prête - Utiliser les fonctions directement ou via requests")
```

**Pour tester dans le notebook :**

```python
# Cellule 3 : Tester l'API
from config_client import ConfigClient

client = ConfigClient(api_url="http://localhost:5000")

# Health check
health = client.health_check()
print(health)

# Récupérer config dev
config = client.get_config("dev")
print(config)
```

### Option 2 : Via Databricks Job (Production)

**Créer un Job Databricks :**

1. **Jobs** → **Create Job**
2. **Task name** : `Config_API_Server`
3. **Type** : Python script
4. **Script** : `/Workspace/Users/you@company.com/wax/api/start_api.py`
5. **Cluster** : Cluster existant ou nouveau (Standard_DS3_v2 suffit)
6. **Schedule** : Always running (ou on-demand)

**Script start_api.py pour Databricks :**

```python
import os
import sys
from flask import Flask

# Configuration
os.environ['CONFIG_FILE_PATH'] = '/dbfs/Volumes/abu_catalog/databricksassetbundletest/externalvolumetes/input/config/config_params.json'
os.environ['API_PORT'] = '8080'

# Importer l'API
sys.path.append("/dbfs/Workspace/Users/you@company.com/wax/api")
from config_api import app

if __name__ == '__main__':
    print("🚀 API Configuration WAX - Databricks")
    
    # Démarrer sur le port du cluster
    app.run(
        host='0.0.0.0',
        port=8080,
        debug=False  # Production mode
    )
```

### Option 3 : Via Databricks Serving (Serverless - Avancé)

Pour une solution serverless, utiliser **Databricks Model Serving** :

```python
# Créer un endpoint serverless
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

w.serving_endpoints.create(
    name="wax-config-api",
    config={
        "served_models": [{
            "model_name": "config_api",
            "model_version": "1",
            "workload_size": "Small",
            "scale_to_zero_enabled": True
        }]
    }
)
```

---

## 🔗 ÉTAPE 3 : Obtenir l'URL de l'API

### Si API via Notebook/Job

L'URL dépend du cluster :

```
http://<cluster-id>.<region>.azuredatabricks.net:8080
```

**Trouver l'URL :**

1. Cluster → Configuration
2. Noter le **Cluster ID** et la **Region**
3. URL complète : `http://<cluster-id>.<region>.azuredatabricks.net:8080`

### Si API via Databricks Serving

```
https://<workspace-url>/serving-endpoints/wax-config-api/invocations
```

---

## 🧪 ÉTAPE 4 : Tester avec Postman

### 1. Configurer l'authentification (si nécessaire)

**Databricks Token Auth :**

- Type : Bearer Token
- Token : Votre Databricks Personal Access Token

### 2. Tester les endpoints

```
GET http://<cluster-url>:8080/health
GET http://<cluster-url>:8080/config/dev
POST http://<cluster-url>:8080/config/test
```

**Headers :**
```
Authorization: Bearer dapi...
Content-Type: application/json
```

---

## 💻 ÉTAPE 5 : Utiliser dans votre Code

### Modifier vos scripts existants

**Avant (valeurs en dur) :**

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

**Après (depuis API) :**

```python
from config import create_config_from_api
import os

# Définir l'URL de l'API
os.environ['CONFIG_API_URL'] = 'http://<cluster-url>:8080'

# Créer config depuis API
config = create_config_from_api(env="dev")

# Utiliser normalement
config.print_config()
```

### Dans vos notebooks Databricks

```python
# Cellule 1 : Configuration
%pip install requests

# Cellule 2 : Charger config depuis API
import os
os.environ['CONFIG_API_URL'] = 'http://localhost:8080'  # Local au cluster

from config import create_config_from_api

# Récupérer config pour l'environnement souhaité
config = create_config_from_api(env=dbutils.widgets.get("env"))

# Afficher
config.print_config()

# Cellule 3 : Lancer le pipeline
from main import main as waxng_ingestion

result = waxng_ingestion(config)
```

---

## 🔐 SÉCURITÉ

### Protection de l'API

**Option 1 : Databricks Network Security**

- Utiliser VPC privé
- Configurer Security Groups
- Limiter accès aux IPs autorisées

**Option 2 : API Key Authentication**

Modifier `config_api.py` pour ajouter l'authentification :

```python
from functools import wraps
from flask import request, jsonify

API_KEY = os.getenv('CONFIG_API_KEY', 'your-secret-key')

def require_api_key(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        key = request.headers.get('X-API-Key')
        if key != API_KEY:
            return jsonify({"error": "Invalid API key"}), 401
        return f(*args, **kwargs)
    return decorated

@app.route('/config/<env>', methods=['GET'])
@require_api_key
def get_config(env):
    # ...
```

---

## 📊 MONITORING

### Logs de l'API

**Via Databricks Job Logs :**

1. Jobs → Votre job API
2. Runs → Latest run
3. Logs → Driver logs

**Via Code :**

```python
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/dbfs/Volumes/.../logs/api.log'),
        logging.StreamHandler()
    ]
)
```

---

## 🔄 MISE À JOUR DES CONFIGURATIONS

### Via Postman

```
POST http://<cluster-url>:8080/config/dev
Body: { "catalog": "new_value", ... }
```

### Via Code

```python
from config_client import ConfigClient

client = ConfigClient(api_url="http://<cluster-url>:8080")

client.create_or_update_config("dev", {
    "catalog": "abu_catalog_v2",
    "schema_files": "databricksassetbundletest_v2",
    "volume": "externalvolumetes_v2",
    "schema_tables": "gdp_poc_dev_v2",
    "env": "dev",
    "version": "v2"
})
```

### Via Notebook UI

Créer un notebook avec widgets pour modifier la config :

```python
dbutils.widgets.text("env", "dev", "Environment")
dbutils.widgets.text("catalog", "", "Catalog")
dbutils.widgets.text("schema_tables", "", "Schema Tables")

# Bouton pour sauvegarder
env = dbutils.widgets.get("env")
catalog = dbutils.widgets.get("catalog")
schema_tables = dbutils.widgets.get("schema_tables")

# Mettre à jour via API
from config_client import ConfigClient
client = ConfigClient()
client.create_or_update_config(env, {
    "catalog": catalog,
    "schema_tables": schema_tables,
    # ... autres champs
})
```

---

## ✅ CHECKLIST DÉPLOIEMENT

- [ ] Fichiers uploadés dans le volume Unity Catalog
- [ ] `config_params.json` créé et accessible
- [ ] API démarrée (Job ou Notebook)
- [ ] URL de l'API obtenue
- [ ] Tests Postman réussis
- [ ] `config.py` modifié pour utiliser l'API
- [ ] Variable `CONFIG_API_URL` définie
- [ ] Tests du pipeline avec la nouvelle config
- [ ] Sécurité configurée (API key ou network)
- [ ] Monitoring en place

---

## 🐛 DÉPANNAGE

### Problème : API ne démarre pas

**Vérifier :**
- Flask installé : `%pip install flask`
- Ports disponibles
- Logs du cluster

### Problème : Config file not found

**Vérifier :**
- Chemin du fichier : `/Volumes/...`
- Permissions sur le volume
- Variable `CONFIG_FILE_PATH`

### Problème : Timeout connexion

**Vérifier :**
- Cluster actif
- Port ouvert (8080)
- Firewall / Security Groups

---

## 📚 RESSOURCES

- [Databricks Jobs](https://docs.databricks.com/jobs/)
- [Unity Catalog Volumes](https://docs.databricks.com/data-governance/unity-catalog/volumes.html)
- [Flask Documentation](https://flask.palletsprojects.com/)

---

✅ **API prête pour production !**

# 🚀 WAX Data Ingestion Pipeline – Version Databricks Production

Ce projet est une solution complète d'ingestion de données ZIP + Excel + CSV vers Delta Lake dans Unity Catalog, avec déclenchement automatisé via API FastAPI, configuration dynamique par `spark.conf`, et portabilité totale entre environnements (dev, prod...).

---

## 📦 Contenu du projet

```
projet_wax/
├── api/                       # API FastAPI pour lancer les jobs Databricks
│   ├── app.py
│   ├── .env
│   ├── requirements.txt
│   └── README.md
├── scripts/
│   ├── run_pipeline.py        # Lancement principal
│   └── run_local.py           # Lancement pour test local (spark local mode)
├── src/wax_pipeline/          # Modules Python
│   ├── config.py              # Configuration basée sur spark.conf
│   ├── ingestion.py           # Orchestration du pipeline
│   ├── file_processor.py      # Chargement des fichiers
│   ├── delta_manager.py       # Sauvegarde Delta / Unity Catalog
│   ├── utils.py, validator.py, etc.
├── tests/
│   └── test_validator.py
├── azure-pipelines.yml        # Pipeline CI/CD Azure DevOps
└── requirements.txt           # Dépendances globales
```

---

## ⚙️ Prérequis

- Workspace **Databricks (Unity Catalog)**
- Un cluster compatible UC
- Fichiers d’entrée dans `/Volumes/<catalog>/<schema>/<volume>/`
- Token personnel Databricks (pour l’API)
- Job Databricks déjà créé (voir ci-dessous)

---

## 📁 Structure des fichiers d'entrée attendus

- `site.zip` : archive contenant des fichiers `.csv`
- `site_config.xlsx` : fichier Excel contenant la configuration de mapping CSV → tables Delta

---

## 🚀 Lancement via Databricks Jobs

### 🎯 Paramètres à passer au job (via UI ou API)

```json
{
  "spark_conf": {
    "catalog_name": "dev_catalog",
    "schema_name": "bronze",
    "volume_name": "landing",
    "env": "dev",
    "zip_path": "/Volumes/dev_catalog/bronze/landing/site.zip",
    "excel_path": "/Volumes/dev_catalog/bronze/landing/site_config.xlsx"
  }
}
```

🧠 Ces paramètres sont récupérés automatiquement via `spark.conf.get()` dans `config.py`.

---

## 🌐 Déclenchement via API FastAPI

### ▶️ Lancer l’API :

```bash
cd api
uvicorn app:app --reload
```

### 🧪 Tester avec Postman :

- **Méthode** : `POST`
- **URL** : `http://localhost:8000/launch-pipeline`
- **Body** :

```json
{
  "catalog": "dev_catalog",
  "schema": "bronze",
  "volume": "landing",
  "env": "dev",
  "zip_path": "/Volumes/dev_catalog/bronze/landing/site.zip",
  "excel_path": "/Volumes/dev_catalog/bronze/landing/site_config.xlsx"
}
```

L’API appelle ensuite le **Job Databricks** en lui passant les paramètres `spark_conf`.

---

## ✅ Fonctionnalités clés

- 🔄 Ingestion automatique de fichiers `.csv` contenus dans une archive `.zip`
- 🧠 Mapping via `site_config.xlsx`
- 🪵 Logging qualité + erreurs
- 🧪 Validation : types, formats, valeurs obligatoires, doublons
- 🔗 Sauvegarde en Delta + Unity Catalog
- 🌍 Portabilité multi-environnement (dev, staging, prod)
- 🌐 Déclenchement via API
- ☁️ Prêt pour Azure DevOps

---

## 🧪 Tests

Lancer les tests unitaires :

```bash
pytest tests/
```

---

## 🛠️ Pipeline CI/CD (Azure DevOps)

Le fichier `azure-pipelines.yml` :

- Installe les dépendances
- Lance les tests
- Déploie et vérifie l’API FastAPI

---

## 🔐 Variables sensibles

Créer un fichier `.env` dans `api/` :

```env
DATABRICKS_TOKEN=xxxxx
DATABRICKS_INSTANCE=https://<instance>.cloud.databricks.com
DATABRICKS_JOB_ID=1234
```

---

## 🧠 Astuce : Surveiller un run via API

```bash
GET https://<instance>.cloud.databricks.com/api/2.1/jobs/runs/get?run_id=98765
Authorization: Bearer <your_token>
```

---

## 📫 Contact

Ce projet a été conçu pour être modulaire, évolutif et déployable dans un contexte Data Engineering professionnel.  
N’hésite pas à l’adapter à tes besoins !


# 🚀 API FastAPI pour lancer un Job Databricks (Unity Catalog compatible)

Ce projet permet de déclencher dynamiquement un pipeline Databricks via une API REST.

## 📦 Installation

```bash
pip install -r requirements.txt
```

## ▶️ Lancement

```bash
uvicorn app:app --reload
```

## 📤 Exemple de requête

```bash
curl -X POST http://localhost:8000/launch-pipeline \
  -H "Content-Type: application/json" \
  -d '{
        "catalog": "dev_catalog",
        "schema": "bronze",
        "volume": "landing",
        "env": "dev",
        "zip_path": "/Volumes/dev_catalog/bronze/landing/site.zip",
        "excel_path": "/Volumes/dev_catalog/bronze/landing/site_config.xlsx"
      }'
```

## 🔐 Sécurité

Configure les variables dans `.env` ou comme variables d'environnement :
- `DATABRICKS_TOKEN`
- `DATABRICKS_INSTANCE`
- `DATABRICKS_JOB_ID`

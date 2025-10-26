# 🧪 GUIDE POSTMAN - API Configuration WAX

## 📋 Prérequis

1. Démarrer l'API sur Databricks ou en local
2. Ouvrir Postman
3. URL de base : `http://localhost:5000` (ou IP du cluster Databricks)

---

## 🔗 COLLECTION POSTMAN - Requêtes

### 1️⃣  **Health Check**

Vérifier que l'API fonctionne

```
GET http://localhost:5000/health
```

**Résultat attendu :**
```json
{
  "status": "healthy",
  "service": "WAX Config API",
  "timestamp": "2025-01-15T10:30:00",
  "config_file": "/dbfs/Volumes/abu_catalog/.../config_params.json",
  "config_exists": true
}
```

---

### 2️⃣  **Lister toutes les configurations**

Récupérer toutes les configs disponibles

```
GET http://localhost:5000/config
```

**Résultat attendu :**
```json
{
  "status": "success",
  "data": {
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
  },
  "timestamp": "2025-01-15T10:30:00"
}
```

---

### 3️⃣  **Récupérer config DEV**

```
GET http://localhost:5000/config/dev
```

**Résultat attendu :**
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
  },
  "timestamp": "2025-01-15T10:30:00"
}
```

---

### 4️⃣  **Récupérer config INT**

```
GET http://localhost:5000/config/int
```

---

### 5️⃣  **Récupérer config PRD**

```
GET http://localhost:5000/config/prd
```

---

### 6️⃣  **Créer/Mettre à jour config TEST**

```
POST http://localhost:5000/config/test
Content-Type: application/json
```

**Body (JSON) :**
```json
{
  "catalog": "test_catalog",
  "schema_files": "test_schema_files",
  "volume": "test_volume",
  "schema_tables": "test_schema_tables",
  "env": "test",
  "version": "v1"
}
```

**Résultat attendu :**
```json
{
  "status": "success",
  "message": "Configuration for 'test' updated successfully",
  "data": {
    "catalog": "test_catalog",
    "schema_files": "test_schema_files",
    "volume": "test_volume",
    "schema_tables": "test_schema_tables",
    "env": "test",
    "version": "v1"
  },
  "timestamp": "2025-01-15T10:30:00"
}
```

---

### 7️⃣  **Modifier config DEV**

```
POST http://localhost:5000/config/dev
Content-Type: application/json
```

**Body (JSON) :**
```json
{
  "catalog": "abu_catalog_new",
  "schema_files": "databricksassetbundletest_v2",
  "volume": "externalvolumetes_v2",
  "schema_tables": "gdp_poc_dev_v2",
  "env": "dev",
  "version": "v2"
}
```

---

### 8️⃣  **Valider config DEV**

```
GET http://localhost:5000/config/dev/validate
```

**Résultat attendu :**
```json
{
  "status": "success",
  "validation": {
    "valid": true,
    "issues": [],
    "config": {
      "catalog": "abu_catalog",
      "schema_files": "databricksassetbundletest",
      "volume": "externalvolumetes",
      "schema_tables": "gdp_poc_dev",
      "env": "dev",
      "version": "v1"
    }
  },
  "timestamp": "2025-01-15T10:30:00"
}
```

---

### 9️⃣  **Supprimer config TEST**

```
DELETE http://localhost:5000/config/test
```

**Résultat attendu :**
```json
{
  "status": "success",
  "message": "Configuration for 'test' deleted successfully",
  "timestamp": "2025-01-15T10:30:00"
}
```

---

### 🔟 **Tester config invalide (erreur)**

```
GET http://localhost:5000/config/nonexistent
```

**Résultat attendu (404) :**
```json
{
  "status": "error",
  "message": "Environment 'nonexistent' not found",
  "available_environments": ["dev", "int", "prd"]
}
```

---

## 📦 IMPORTER LA COLLECTION POSTMAN

### Option 1 : Import JSON

Créer un fichier `WAX_Config_API.postman_collection.json` :

```json
{
  "info": {
    "name": "WAX Config API",
    "schema": "https://schema.getpostman.com/json/collection/v2.1.0/collection.json"
  },
  "item": [
    {
      "name": "Health Check",
      "request": {
        "method": "GET",
        "header": [],
        "url": {
          "raw": "http://localhost:5000/health",
          "protocol": "http",
          "host": ["localhost"],
          "port": "5000",
          "path": ["health"]
        }
      }
    },
    {
      "name": "Get All Configs",
      "request": {
        "method": "GET",
        "header": [],
        "url": {
          "raw": "http://localhost:5000/config",
          "protocol": "http",
          "host": ["localhost"],
          "port": "5000",
          "path": ["config"]
        }
      }
    },
    {
      "name": "Get DEV Config",
      "request": {
        "method": "GET",
        "header": [],
        "url": {
          "raw": "http://localhost:5000/config/dev",
          "protocol": "http",
          "host": ["localhost"],
          "port": "5000",
          "path": ["config", "dev"]
        }
      }
    },
    {
      "name": "Create/Update TEST Config",
      "request": {
        "method": "POST",
        "header": [
          {
            "key": "Content-Type",
            "value": "application/json"
          }
        ],
        "body": {
          "mode": "raw",
          "raw": "{\n  \"catalog\": \"test_catalog\",\n  \"schema_files\": \"test_schema\",\n  \"volume\": \"test_volume\",\n  \"schema_tables\": \"test_tables\",\n  \"env\": \"test\",\n  \"version\": \"v1\"\n}"
        },
        "url": {
          "raw": "http://localhost:5000/config/test",
          "protocol": "http",
          "host": ["localhost"],
          "port": "5000",
          "path": ["config", "test"]
        }
      }
    },
    {
      "name": "Validate DEV Config",
      "request": {
        "method": "GET",
        "header": [],
        "url": {
          "raw": "http://localhost:5000/config/dev/validate",
          "protocol": "http",
          "host": ["localhost"],
          "port": "5000",
          "path": ["config", "dev", "validate"]
        }
      }
    },
    {
      "name": "Delete TEST Config",
      "request": {
        "method": "DELETE",
        "header": [],
        "url": {
          "raw": "http://localhost:5000/config/test",
          "protocol": "http",
          "host": ["localhost"],
          "port": "5000",
          "path": ["config", "test"]
        }
      }
    }
  ]
}
```

**Importer dans Postman :**
1. Postman → Import
2. Sélectionner le fichier JSON
3. ✅ Collection créée !

---

## 🚀 SCÉNARIOS DE TEST

### Scénario 1 : Vérification initiale

1. **Health Check** → Vérifier que l'API fonctionne
2. **Get All Configs** → Voir les configs existantes
3. **Get DEV Config** → Récupérer la config dev

### Scénario 2 : Création d'un nouvel environnement

1. **Create TEST Config** → Créer config test
2. **Get All Configs** → Vérifier que test est présent
3. **Validate TEST Config** → Valider la config
4. **Delete TEST Config** → Supprimer

### Scénario 3 : Modification d'une config

1. **Get DEV Config** → Voir config actuelle
2. **POST /config/dev** → Modifier avec nouvelles valeurs
3. **Get DEV Config** → Vérifier les modifications
4. **Validate DEV Config** → Valider

---

## 🔧 VARIABLES D'ENVIRONNEMENT POSTMAN

Créer des variables pour faciliter les tests :

**Variables :**
- `base_url` : `http://localhost:5000`
- `env` : `dev` (changeable)

**Utilisation dans les requêtes :**
```
{{base_url}}/config/{{env}}
```

---

## 📊 TESTS AUTOMATIQUES POSTMAN

Ajouter dans l'onglet **Tests** de chaque requête :

### Health Check
```javascript
pm.test("Status is 200", function () {
    pm.response.to.have.status(200);
});

pm.test("API is healthy", function () {
    var jsonData = pm.response.json();
    pm.expect(jsonData.status).to.eql("healthy");
});
```

### Get Config
```javascript
pm.test("Status is 200", function () {
    pm.response.to.have.status(200);
});

pm.test("Has required fields", function () {
    var jsonData = pm.response.json();
    pm.expect(jsonData.data).to.have.property("catalog");
    pm.expect(jsonData.data).to.have.property("env");
});
```

---

## 🎯 CHECKLIST TESTS

- [ ] Health check réussit
- [ ] Get all configs retourne les 3 envs (dev/int/prd)
- [ ] Get DEV config retourne les bons paramètres
- [ ] Création config TEST réussit
- [ ] Validation config réussit
- [ ] Suppression config TEST réussit
- [ ] Erreur 404 pour env inexistant
- [ ] Modification d'une config existante fonctionne

---

## 🐛 DÉPANNAGE

### Erreur : Connection refused
→ L'API n'est pas démarrée
```bash
python start_api.py
```

### Erreur : Config file not found
→ Le fichier config_params.json n'existe pas
→ L'API utilisera la config par défaut

### Erreur 500 : Internal server error
→ Vérifier les logs de l'API
→ Vérifier les permissions sur le volume Unity Catalog

---

## 📝 NOTES

- Les modifications sont persistées dans `config_params.json`
- L'API charge automatiquement les changements
- Utiliser le endpoint `/validate` avant de déployer en prod
- Sauvegarder le fichier JSON avant modification importante

---

✅ **Prêt pour les tests !**

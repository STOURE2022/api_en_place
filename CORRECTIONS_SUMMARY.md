# 🔧 CORRECTIONS APPORTÉES AU CODE WAX

## ✅ Fichiers corrigés

### 1. **validator_fixed.py**
**Problème :** Manque validation case-sensitive de la partie fixe du filename

**Ajouts :**
- ✅ Nouvelle fonction `validate_filename_pattern()` 
  - Extrait fix_part du filename
  - Comparaison **strictement case-sensitive** avec pattern défini
  - Rejette si mismatch (ex: "Billing" ≠ "billing")
  - Log erreur avec code "BBAFORGIF-102"

**Exemple :**
```python
# Pattern attendu : "Billing"
# Fichier reçu : "billing_20251028.csv"
# Résultat : ❌ REJET (case mismatch)

validator.validate_filename_pattern(
    fname="billing_20251028.csv",
    filename_pattern="Billing_<yyyy><mm><dd>.csv",
    fix_part_pattern="Billing",  # ← Case-sensitive !
    table_name="site",
    log_quality_path="/path/to/logs"
)
```

---

### 2. **column_processor_fixed.py**
**Problèmes :**
- Fallback sur plusieurs patterns au lieu de validation stricte
- Format sortie = date au lieu de timestamp

**Corrections :**

#### A. Nouvelle fonction `parse_date_strict()`
```python
def parse_date_strict(self, df, cname, pattern, table_name, filename, error_action):
    """
    ✅ Parse dates avec UN SEUL pattern (pas de fallback)
    ✅ Format sortie : TIMESTAMP (yyyy-MM-dd HH:mm:ss)
    ✅ Applique Error Action si échec
    """
```

**Avant :**
```python
# Essayait plusieurs patterns (fallback)
for p in patterns:
    ts_col = cand if ts_col is None else F.coalesce(ts_col, cand)
```

**Après :**
```python
# UN SEUL pattern, pas de fallback
ts_col = F.expr(f"try_to_timestamp({cname}, '{pattern}')")
```

#### B. Gestion Error Action stricte
- Si pattern ne match pas → applique action définie (REJECT, ICT_DRIVEN, LOG_ONLY)
- Si REJECT → supprime les lignes invalides
- Format sortie = **TIMESTAMP** (pas DATE)

**Exemple :**
```
Input : "15/10/2025 10:77:10"
Pattern : "dd/MM/yyyy HH:mm:ss"
Résultat : ❌ Parsing échoué (77 minutes invalide)
Action : → NULL + log erreur + applique Error Action Rule
```

---

### 3. **ingestion_fixed.py**
**Problème :** Nommage des tables hardcodé (toujours `_all` et `_last`)

**Ajouts :**

#### Nouvelle fonction `_get_table_names()`
```python
def _get_table_names(self, base_table_name, table_config):
    """
    ✅ Détermine noms de tables selon configuration Excel
    
    Règles :
    - Si last_table_name_conf défini → utiliser ce nom
    - Sinon → delta_table_name_conf + "_last"
    - Table _all → toujours delta_table_name_conf + "_all"
    """
```

**Scénarios supportés :**

| Config Excel | Résultat |
|--------------|----------|
| `last_table_name_conf = "billing_last_version"` | ✅ Utilise ce nom exact |
| `last_table_name_conf = ""` + `delta_table_name_conf = "billing"` | ✅ Utilise "billing_last" |
| `delta_table_name_conf = "billing"` | ✅ Tables : "billing_all" et "billing_last" |

**Exemple config Excel :**
```
| delta_table_name_conf | last_table_name_conf | → Résultat
|----------------------|----------------------|------------
| billing              | billing_last_version | ✅ billing_last_version
| billing              | (vide)              | ✅ billing_last
```

---

## 📝 Utilisation

### Remplacer les fichiers originaux

1. **validator.py** → **validator_fixed.py**
2. **column_processor.py** → **column_processor_fixed.py**
3. **ingestion.py** → **ingestion_fixed.py**

### Modifications dans votre Excel de config

Ajouter ces colonnes si absentes :
- `last_table_name_conf` (optionnel)
- `delta_table_name_conf` (optionnel)
- `fix_part_filename` (pour validation case-sensitive)

---

## 🎯 Résumé des conformités

| Règle Documentation | Avant | Après |
|---------------------|-------|-------|
| **Validation date filename** | ✅ OK | ✅ OK |
| **Validation fix_part (case-sensitive)** | ❌ Manque | ✅ Ajouté |
| **Parsing date strict (pas fallback)** | ❌ Fallback | ✅ Strict |
| **Format sortie timestamp** | ❌ Date | ✅ Timestamp |
| **Noms tables customisés** | ❌ Hardcodé | ✅ Config Excel |

---

## 🔍 Tests recommandés

1. **Test fix_part case-sensitive :**
   ```
   Pattern : "Billing"
   Fichier : "billing_20251028.csv" → ❌ Doit rejeter
   Fichier : "Billing_20251028.csv" → ✅ Doit accepter
   ```

2. **Test parsing date strict :**
   ```
   Pattern : "dd/MM/yyyy HH:mm:ss"
   Input : "15/10/2025 10:77:10" → ❌ NULL + erreur loguée
   Input : "15/10/2025 10:10:10" → ✅ 2025-10-15 10:10:10
   ```

3. **Test noms tables :**
   ```
   Config : last_table_name_conf = "custom_name"
   Résultat : Tables créées avec nom custom
   ```

---

**Date correction :** 2025-10-28
**Version :** 2.0.1

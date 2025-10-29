"""
autoloader_module.py - VERSION FINALE CORRIGÉE
MODULE 2 : Auto Loader avec normalisation des colonnes
✅ Validation dates filename
✅ Validation fix_part case-sensitive
✅ Filtrage ligne header FIRST_LINE
"""

import json
import os
from collections import defaultdict
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
import re


class AutoLoaderModule:
    """Module Auto Loader avec détection basée sur le contenu (Unity Catalog)"""
    
    def __init__(self, spark, config):
        self.spark = spark
        self.config = config
        
        # Chemins Unity Catalog
        self.source_base = f"{config.volume_base}/extracted"
        self.checkpoint_base = f"{config.volume_base}/checkpoints"
        self.schema_base = f"{config.volume_base}/schemas"
        self.rejected_base = f"{config.volume_base}/rejected"
    
    def process_all_tables(self, excel_config_path: str) -> dict:
        """Lance Auto Loader pour tous les fichiers CSV trouvés"""
        
        print("=" * 80)
        print("🔄 MODULE 2 : AUTO LOADER (CONTENT-BASED DISCOVERY)")
        print("=" * 80)
        
        # Lire configuration Excel
        import pandas as pd
        
        print(f"\n📖 Lecture configuration : {excel_config_path}")
        
        try:
            file_tables_df = pd.read_excel(excel_config_path, sheet_name="File-Table")
            file_columns_df = pd.read_excel(excel_config_path, sheet_name="Field-Column")
            print(f"✅ Configuration chargée : {len(file_tables_df)} table(s)\n")
        except Exception as e:
            print(f"❌ Erreur lecture Excel : {e}")
            import traceback
            traceback.print_exc()
            return {"status": "ERROR", "error": str(e)}
        
        # Découverte des fichiers CSV
        print(f"🔍 Scan récursif du répertoire : {self.source_base}")
        
        files_by_table = self._discover_csv_files(self.source_base)
        
        if not files_by_table:
            print(f"⚠️  Aucun fichier CSV trouvé dans extracted/")
            return {"status": "NO_DATA", "message": "No CSV files found"}
        
        print(f"\n✅ {len(files_by_table)} table(s) détectée(s) depuis les fichiers CSV :")
        for table_name, files_info in files_by_table.items():
            print(f"   • {table_name}: {len(files_info['files'])} fichier(s)")
        
        # ✅ VALIDATION 1 : Dates dans noms de fichiers
        print(f"\n🔍 Validation des dates dans les noms de fichiers...")
        files_by_table, rejected_files_dates = self._validate_file_dates(files_by_table)
        
        if rejected_files_dates:
            print(f"\n🚫 {len(rejected_files_dates)} fichier(s) rejeté(s) (dates invalides) :")
            for rejected in rejected_files_dates[:10]:
                print(f"   ❌ {rejected['filename']} : {rejected['reason']}")
            if len(rejected_files_dates) > 10:
                print(f"   ... et {len(rejected_files_dates) - 10} autre(s)")
        
        # ✅ VALIDATION 2 (NOUVEAU) : Fix_part case-sensitive
        print(f"\n🔍 Validation fix_part (case-sensitive)...")
        files_by_table, rejected_files_fixpart = self._validate_fix_part(
            files_by_table, file_tables_df
        )
        
        if rejected_files_fixpart:
            print(f"\n🚫 {len(rejected_files_fixpart)} fichier(s) rejeté(s) (fix_part mismatch) :")
            for rejected in rejected_files_fixpart[:10]:
                print(f"   ❌ {rejected['filename']} : {rejected['reason']}")
            if len(rejected_files_fixpart) > 10:
                print(f"   ... et {len(rejected_files_fixpart) - 10} autre(s)")
        
        # Déplacer tous les fichiers rejetés
        all_rejected = rejected_files_dates + rejected_files_fixpart
        if all_rejected:
            self._move_rejected_files(all_rejected)
        
        # Traiter chaque table
        results = []
        success_count = 0
        failed_count = 0
        total_rows = 0
        
        for idx, (table_name, files_info) in enumerate(files_by_table.items(), 1):
            
            print(f"\n{'=' * 80}")
            print(f"📋 Table {idx}/{len(files_by_table)}: {table_name}")
            print(f"{'=' * 80}")
            
            # Chercher config
            table_config = self._find_table_config(table_name, file_tables_df)
            
            if table_config is None:
                print(f"⚠️  Aucune configuration trouvée dans Excel pour '{table_name}'")
                print(f"   → Utilisation de la configuration par défaut")
                
                table_config = {
                    "Delta Table Name": table_name,
                    "Input Format": "csv",
                    "Input delimiter": ",",
                    "Input charset": "UTF-8",
                    "Input header": "HEADER_USE"
                }
            else:
                print(f"✅ Configuration trouvée : table '{table_config['Delta Table Name']}'")
            
            print(f"📁 {len(files_info['files'])} fichier(s) CSV valides :")
            for file_path in files_info['files'][:5]:
                print(f"   • {os.path.basename(file_path)}")
            if len(files_info['files']) > 5:
                print(f"   ... et {len(files_info['files']) - 5} autre(s)")
            
            # Colonnes
            if isinstance(table_config, dict):
                config_table_name = table_config["Delta Table Name"]
            else:
                config_table_name = table_config["Delta Table Name"]
            
            table_columns = file_columns_df[
                file_columns_df["Delta Table Name"] == config_table_name
            ]
            
            # Traiter
            try:
                result = self._process_single_table(
                    table_name,
                    files_info,
                    table_config, 
                    table_columns
                )
                results.append(result)
                
                if result["status"] == "SUCCESS":
                    print(f"✅ {result.get('rows_ingested', 0):,} ligne(s) ingérée(s)")
                    success_count += 1
                    total_rows += result.get('rows_ingested', 0)
                elif result["status"] == "NO_DATA":
                    print(f"⚠️  Aucune nouvelle donnée")
                else:
                    print(f"❌ Échec : {result.get('error', 'Unknown')}")
                    failed_count += 1
                    
            except Exception as e:
                print(f"❌ Erreur : {e}")
                import traceback
                traceback.print_exc()
                
                failed_count += 1
                results.append({
                    "table": table_name,
                    "status": "ERROR",
                    "error": str(e)
                })
        
        # Résumé
        print("\n" + "=" * 80)
        print("📊 RÉSUMÉ AUTO LOADER")
        print("=" * 80)
        print(f"✅ Tables traitées  : {success_count}")
        print(f"❌ Tables en échec  : {failed_count}")
        print(f"🚫 Fichiers rejetés : {len(all_rejected)}")
        print(f"   └─ Dates invalides : {len(rejected_files_dates)}")
        print(f"   └─ Fix_part mismatch : {len(rejected_files_fixpart)}")
        print(f"📈 Total lignes     : {total_rows:,}")
        print("=" * 80)
        
        return {
            "status": "SUCCESS" if failed_count == 0 else "PARTIAL",
            "success_count": success_count,
            "failed_count": failed_count,
            "rejected_count": len(all_rejected),
            "total_rows": total_rows,
            "results": results
        }
    
    # ==================== ✅ NOUVELLE FONCTION : VALIDATION FIX_PART ====================
    
    def _validate_fix_part(self, files_by_table: dict, file_tables_df) -> tuple:
        """
        ✅ NOUVEAU : Valide fix_part case-sensitive selon pattern Excel
        
        Règle : Si fix_part du filename ne match pas (case-sensitive) le pattern → REJECT
        Exemple : Pattern "Billing" != filename "billing_20251028.csv"
        
        Returns:
            (files_by_table_valid, rejected_files)
        """
        
        rejected_files = []
        files_by_table_valid = defaultdict(lambda: {"files": [], "folders": set()})
        
        for table_name, files_info in files_by_table.items():
            
            # Chercher pattern attendu dans Excel
            table_config = self._find_table_config(table_name, file_tables_df)
            
            if table_config is None:
                # Pas de config → accepter tous les fichiers
                files_by_table_valid[table_name]["files"].extend(files_info["files"])
                files_by_table_valid[table_name]["folders"].update(files_info["folders"])
                continue
            
            # Récupérer filename pattern
            if isinstance(table_config, dict):
                filename_pattern = str(table_config.get("Filename Pattern", "")).strip()
            else:
                filename_pattern = str(table_config.get("Filename Pattern", "")).strip()
            
            if not filename_pattern or filename_pattern.lower() in ["", "nan", "none"]:
                # Pas de pattern défini → accepter tous
                files_by_table_valid[table_name]["files"].extend(files_info["files"])
                files_by_table_valid[table_name]["folders"].update(files_info["folders"])
                continue
            
            # Extraire fix_part du pattern
            # Ex: "Billing_<yyyy><mm><dd>.csv" → "Billing"
            fix_part_expected = self._extract_fix_part_from_pattern(filename_pattern)
            
            if not fix_part_expected:
                # Pattern sans fix_part (ex: "<yyyy><mm><dd>.csv") → accepter tous
                files_by_table_valid[table_name]["files"].extend(files_info["files"])
                files_by_table_valid[table_name]["folders"].update(files_info["folders"])
                continue
            
            # Valider chaque fichier
            for file_path in files_info["files"]:
                filename = os.path.basename(file_path)
                
                # Extraire fix_part du filename
                # Ex: "Billing_20251028.csv" → "Billing"
                fix_part_actual = self._extract_fix_part_from_filename(filename)
                
                # ✅ Comparaison CASE-SENSITIVE
                if fix_part_actual == fix_part_expected:
                    # Match → accepter
                    files_by_table_valid[table_name]["files"].append(file_path)
                    files_by_table_valid[table_name]["folders"].add(os.path.dirname(file_path))
                else:
                    # Mismatch → rejeter
                    rejected_files.append({
                        "filename": filename,
                        "filepath": file_path,
                        "table": table_name,
                        "fix_part_expected": fix_part_expected,
                        "fix_part_actual": fix_part_actual,
                        "reason": f"Fix_part mismatch (case-sensitive): expected '{fix_part_expected}', got '{fix_part_actual}'"
                    })
        
        # Convertir folders sets en lists
        for table_name in files_by_table_valid:
            files_by_table_valid[table_name]["folders"] = list(files_by_table_valid[table_name]["folders"])
        
        return dict(files_by_table_valid), rejected_files
    
    def _extract_fix_part_from_pattern(self, pattern: str) -> str:
        """
        Extrait la partie fixe d'un pattern
        
        Ex: "Billing_<yyyy><mm><dd>.csv" → "Billing"
        Ex: "<yyyy><mm><dd>_data.csv" → "" (pas de fix_part au début)
        """
        
        # Supprimer les placeholders
        pattern_clean = re.sub(r'<[^>]+>', '', pattern)
        
        # Supprimer extension
        pattern_clean = pattern_clean.replace('.csv', '').replace('.CSV', '')
        
        # Prendre la partie avant le premier underscore ou point
        if '_' in pattern_clean:
            fix_part = pattern_clean.split('_')[0]
        else:
            fix_part = pattern_clean
        
        return fix_part.strip()
    
    def _extract_fix_part_from_filename(self, filename: str) -> str:
        """
        Extrait la partie fixe d'un filename
        
        Ex: "Billing_20251028.csv" → "Billing"
        Ex: "site.csv" → "site"
        """
        
        # Supprimer extension
        basename = os.path.splitext(filename)[0]
        
        # Prendre la partie avant le premier underscore ou chiffre
        if '_' in basename:
            parts = basename.split('_')
            # Prendre la première partie non-numérique
            for part in parts:
                if not part.isdigit():
                    return part
            return parts[0]
        
        # Supprimer les chiffres à la fin
        fix_part = re.sub(r'\d+$', '', basename)
        
        return fix_part.strip()
    
    # ==================== VALIDATION DATES (DÉJÀ PRÉSENT) ====================
    
    def _validate_file_dates(self, files_by_table: dict) -> tuple:
        """
        Valide les dates dans les noms de fichiers
        
        Returns:
            (files_by_table_valid, rejected_files)
        """
        
        rejected_files = []
        files_by_table_valid = defaultdict(lambda: {"files": [], "folders": set()})
        
        for table_name, files_info in files_by_table.items():
            for file_path in files_info["files"]:
                filename = os.path.basename(file_path)
                
                # Extraire date du nom de fichier
                date_match = re.search(r"(\d{4})(\d{2})(\d{2})", filename)
                
                if not date_match:
                    # Pas de date → on accepte (fichiers génériques comme "site.csv")
                    files_by_table_valid[table_name]["files"].append(file_path)
                    files_by_table_valid[table_name]["folders"].add(os.path.dirname(file_path))
                    continue
                
                yyyy = int(date_match.group(1))
                mm = int(date_match.group(2))
                dd = int(date_match.group(3))
                
                # Validation basique
                is_valid = True
                reason = None
                
                if not (1900 <= yyyy <= 2100):
                    is_valid = False
                    reason = f"Année invalide : {yyyy} (doit être entre 1900 et 2100)"
                elif not (1 <= mm <= 12):
                    is_valid = False
                    reason = f"Mois invalide : {mm} (doit être entre 1 et 12)"
                elif not (1 <= dd <= 31):
                    is_valid = False
                    reason = f"Jour invalide : {dd} (doit être entre 1 et 31)"
                else:
                    # Vérifier que la date est valide (ex: pas 31 février)
                    try:
                        datetime(yyyy, mm, dd)
                    except ValueError as e:
                        is_valid = False
                        reason = f"Date invalide : {yyyy}-{mm:02d}-{dd:02d} ({e})"
                
                if is_valid:
                    # Date valide → accepter
                    files_by_table_valid[table_name]["files"].append(file_path)
                    files_by_table_valid[table_name]["folders"].add(os.path.dirname(file_path))
                else:
                    # Date invalide → rejeter
                    rejected_files.append({
                        "filename": filename,
                        "filepath": file_path,
                        "table": table_name,
                        "yyyy": yyyy,
                        "mm": mm,
                        "dd": dd,
                        "reason": reason
                    })
        
        # Convertir folders sets en lists
        for table_name in files_by_table_valid:
            files_by_table_valid[table_name]["folders"] = list(files_by_table_valid[table_name]["folders"])
        
        return dict(files_by_table_valid), rejected_files
    
    def _move_rejected_files(self, rejected_files: list):
        """Déplace les fichiers rejetés vers rejected/"""
        import shutil
        
        # Créer répertoire rejected si nécessaire
        rejected_dir_fs = self.rejected_base.replace("/Volumes", "/dbfs/Volumes")
        os.makedirs(rejected_dir_fs, exist_ok=True)
        
        for rejected in rejected_files:
            try:
                source = rejected["filepath"].replace("/Volumes", "/dbfs/Volumes")
                
                # Créer sous-répertoire par table
                table_rejected_dir = os.path.join(rejected_dir_fs, rejected["table"])
                os.makedirs(table_rejected_dir, exist_ok=True)
                
                # Destination avec timestamp
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                dest_filename = f"{rejected['filename']}.rejected_{timestamp}"
                dest = os.path.join(table_rejected_dir, dest_filename)
                
                # Déplacer
                shutil.move(source, dest)
                print(f"   🗑️  Déplacé : {rejected['filename']} → rejected/{rejected['table']}/")
                
                # Créer fichier .reason avec la raison du rejet
                reason_file = dest + ".reason"
                with open(reason_file, "w") as f:
                    f.write(f"Fichier : {rejected['filename']}\n")
                    f.write(f"Raison : {rejected['reason']}\n")
                    f.write(f"Timestamp rejet : {timestamp}\n")
                    
                    # Infos spécifiques selon type de rejet
                    if "yyyy" in rejected:
                        f.write(f"Date extraite : {rejected['yyyy']}-{rejected['mm']:02d}-{rejected['dd']:02d}\n")
                    if "fix_part_expected" in rejected:
                        f.write(f"Fix_part attendu : {rejected['fix_part_expected']}\n")
                        f.write(f"Fix_part trouvé : {rejected['fix_part_actual']}\n")
                
            except Exception as e:
                print(f"   ⚠️  Erreur déplacement {rejected['filename']} : {e}")
    
    # ==================== RESTE DU CODE INCHANGÉ ====================
    
    def _discover_csv_files(self, base_dir: str) -> dict:
        """Découvre tous les fichiers CSV et les groupe par table"""
        
        files_by_table = defaultdict(lambda: {"files": [], "folders": set()})
        
        if not os.path.exists(base_dir):
            return {}
        
        for root, dirs, files in os.walk(base_dir):
            dirs[:] = [d for d in dirs if not d.startswith('.')]
            
            for file in files:
                if file.startswith('.') or not file.lower().endswith('.csv'):
                    continue
                
                table_name = self._extract_table_name(file)
                
                if table_name:
                    file_path = os.path.join(root, file)
                    folder_path = root
                    
                    files_by_table[table_name]["files"].append(file_path)
                    files_by_table[table_name]["folders"].add(folder_path)
        
        for table_name in files_by_table:
            files_by_table[table_name]["folders"] = list(files_by_table[table_name]["folders"])
        
        return dict(files_by_table)
    
    def _extract_table_name(self, filename: str) -> str:
        """Extrait le nom de la table depuis le nom du fichier"""
        
        basename = os.path.splitext(filename)[0]
        
        if '_' in basename:
            parts = basename.split('_')
            for part in parts:
                if not part.isdigit():
                    return part.lower()
            return parts[0].lower()
        
        return basename.lower()
    
    def _find_table_config(self, table_name: str, file_tables_df) -> dict:
        """Cherche la configuration correspondante"""
        
        table_lower = table_name.lower()
        
        for idx, row in file_tables_df.iterrows():
            config_table = str(row["Delta Table Name"]).strip().lower()
            
            if config_table == table_lower:
                return row
            
            if table_lower in config_table or config_table in table_lower:
                return row
        
        return None
    
    def _process_single_table(self, table_name: str, files_info: dict, 
                              table_config, columns_config) -> dict:
        """Traite une table avec Auto Loader (Unity Catalog)"""
        
        folders = files_info["folders"]
        
        if len(folders) == 1:
            source_path = folders[0]
        else:
            source_path = os.path.commonpath(folders)
        
        checkpoint_path = f"{self.checkpoint_base}/{table_name}"
        schema_path = f"{self.schema_base}/{table_name}"
        
        if isinstance(table_config, dict):
            config_table_name = table_config["Delta Table Name"]
        else:
            config_table_name = table_config["Delta Table Name"]
        
        target_table = f"{self.config.catalog}.{self.config.schema_tables}.{config_table_name}_staging"
        
        print(f"\n📂 Source(s)   : {source_path}")
        print(f"📂 Checkpoint  : {checkpoint_path}")
        print(f"🗄️  Target      : {target_table}")
        
        # Lire config
        if isinstance(table_config, dict):
            input_format = str(table_config.get("Input Format", "csv")).strip().lower()
            delimiter = str(table_config.get("Input delimiter", ","))
            charset = str(table_config.get("Input charset", "UTF-8")).strip()
            input_header = str(table_config.get("Input header", "")).strip().upper()
        else:
            input_format = str(table_config.get("Input Format", "csv")).strip().lower()
            delimiter = str(table_config.get("Input delimiter", ","))
            charset = str(table_config.get("Input charset", "UTF-8")).strip()
            input_header = str(table_config.get("Input header", "")).strip().upper()
        
        if charset.lower() in ["nan", "", "none"]:
            charset = "UTF-8"
        
        # Options Auto Loader
        options = {
            "cloudFiles.format": input_format,
            "cloudFiles.useNotifications": "false",
            "cloudFiles.includeExistingFiles": "true",
            "cloudFiles.schemaLocation": schema_path,
        }
        
        # Options CSV selon mode header
        if input_format in ["csv", "csv_quote", "csv_quote_ml"]:
            
            if input_header == "FIRST_LINE":
                options.update({
                    "header": "false",
                    "delimiter": delimiter,
                    "encoding": charset,
                    "inferSchema": "false",
                    "mode": "PERMISSIVE",
                    "columnNameOfCorruptRecord": "_corrupt_record",
                    "quote": '"',
                    "escape": "\\"
                })
            else:
                options.update({
                    "header": "true",
                    "delimiter": delimiter,
                    "encoding": charset,
                    "inferSchema": "false",
                    "mode": "PERMISSIVE",
                    "columnNameOfCorruptRecord": "_corrupt_record",
                    "quote": '"',
                    "escape": "\\"
                })
            
            if input_format == "csv_quote_ml":
                options["multiline"] = "true"
        
        print(f"\n🔄 Création stream Auto Loader...")
        print(f"   Pattern: {table_name}_*.csv")
        print(f"   Input header mode: {input_header}")
        
        try:
            # Créer stream
            df_stream = (
                self.spark.readStream
                .format("cloudFiles")
                .options(**options)
                .load(source_path)
            )
            
            # Normalisation colonnes
            if input_header == "HEADER_USE":
                print(f"   → Mode HEADER_USE : Normalisation en lowercase")
                
                normalized_count = 0
                for col in df_stream.columns:
                    if not col.startswith("_"):
                        col_lower = col.lower()
                        if col != col_lower:
                            df_stream = df_stream.withColumnRenamed(col, col_lower)
                            normalized_count += 1
                
                if normalized_count > 0:
                    print(f"   → {normalized_count} colonne(s) normalisée(s)")
            
            elif input_header == "FIRST_LINE":
                print(f"   → Mode FIRST_LINE : Renommage selon Excel")
                
                if not columns_config.empty:
                    expected_column_names = columns_config["Column Name"].tolist()
                    
                    auto_cols = [c for c in df_stream.columns if c.startswith("_c")]
                    auto_cols_sorted = sorted(auto_cols, key=lambda x: int(x[2:]) if x[2:].isdigit() else 999)
                    
                    for i, auto_col in enumerate(auto_cols_sorted):
                        if i < len(expected_column_names):
                            new_name = expected_column_names[i].lower()
                            df_stream = df_stream.withColumnRenamed(auto_col, new_name)
                            print(f"      {auto_col} → {new_name}")
                    
                    print(f"   → {min(len(auto_cols_sorted), len(expected_column_names))} colonne(s) renommée(s)")
                    
                    # Filtrer ligne header
                    print(f"   → Filtrage de la ligne header")
                    
                    header_conditions = []
                    for expected_col in expected_column_names[:5]:
                        col_lower = expected_col.lower()
                        if col_lower in df_stream.columns:
                            header_conditions.append(
                                F.upper(F.col(col_lower)) == expected_col.upper()
                            )
                    
                    if header_conditions:
                        from functools import reduce
                        import operator
                        
                        header_filter = reduce(operator.and_, header_conditions)
                        df_stream = df_stream.filter(~header_filter)
                        
                        print(f"   → Ligne header filtrée")
            
            else:
                print(f"   → Mode par défaut")
            
            # Filtrer par table
            df_stream = df_stream.filter(
                (F.element_at(F.split(F.col("_metadata.file_path"), "/"), -1).startswith(f"{table_name}_"))
                | (F.element_at(F.split(F.col("_metadata.file_path"), "/"), -1).rlike(f"^{table_name}\\.csv$"))
            )
            
        except Exception as e:
            return {"status": "ERROR", "error": f"Stream creation failed: {e}"}
        
        # Ajouter métadonnées
        df_stream = self._add_metadata(df_stream)
        
        print(f"💾 Écriture vers {target_table}...")
        
        try:
            query = (
                df_stream.writeStream
                .format("delta")
                .outputMode("append")
                .option("checkpointLocation", checkpoint_path)
                .option("mergeSchema", "true")
                .trigger(once=True)
                .toTable(target_table)
            )
            
            print(f"⏳ Traitement en cours...")
            query.awaitTermination()
            
            progress = query.lastProgress
            
            if progress:
                rows_ingested = progress.get("numInputRows", 0)
                
                if rows_ingested > 0:
                    return {
                        "status": "SUCCESS",
                        "rows_ingested": rows_ingested,
                        "target_table": target_table,
                        "source_files": len(files_info["files"])
                    }
                else:
                    return {"status": "NO_DATA", "rows_ingested": 0, "target_table": target_table}
            else:
                return {"status": "NO_DATA", "rows_ingested": 0, "target_table": target_table}
            
        except Exception as e:
            import traceback
            traceback.print_exc()
            return {"status": "ERROR", "error": f"Write failed: {e}"}
    
    def _add_metadata(self, df_stream):
        """Ajoute métadonnées (Unity Catalog compatible)"""
        
        df_stream = df_stream.withColumn(
            "FILE_NAME_RECEIVED",
            F.element_at(F.split(F.col("_metadata.file_path"), "/"), -1)
        )
        
        df_stream = df_stream.withColumn(
            "yyyy",
            F.regexp_extract(F.col("FILE_NAME_RECEIVED"), r"_(\d{4})\d{4}", 1).cast("int")
        )
        
        df_stream = df_stream.withColumn(
            "mm",
            F.regexp_extract(F.col("FILE_NAME_RECEIVED"), r"_\d{4}(\d{2})\d{2}", 1).cast("int")
        )
        
        df_stream = df_stream.withColumn(
            "dd",
            F.regexp_extract(F.col("FILE_NAME_RECEIVED"), r"_\d{6}(\d{2})", 1).cast("int")
        )
        
        df_stream = df_stream.withColumn(
            "INGESTION_TIMESTAMP",
            F.current_timestamp()
        )
        
        df_stream = df_stream.withColumn(
            "FILE_PATH_RECEIVED",
            F.col("_metadata.file_path")
        )
        
        return df_stream
    
    def list_staging_tables(self) -> list:
        """Liste les tables staging"""
        try:
            tables = self.spark.sql(
                f"SHOW TABLES IN {self.config.catalog}.{self.config.schema_tables}"
            ).collect()
            return [t.tableName for t in tables if "_staging" in t.tableName]
        except:
            return []
    
    def get_staging_stats(self) -> dict:
        """Stats des tables staging"""
        staging_tables = self.list_staging_tables()
        stats = {}
        
        for table_name in staging_tables:
            table_full = f"{self.config.catalog}.{self.config.schema_tables}.{table_name}"
            try:
                df = self.spark.table(table_full)
                count = df.count()
                sources = []
                if "FILE_NAME_RECEIVED" in df.columns:
                    sources = [row.FILE_NAME_RECEIVED 
                             for row in df.select("FILE_NAME_RECEIVED").distinct().collect()]
                stats[table_name] = {"rows": count, "sources": sources}
            except Exception as e:
                stats[table_name] = {"error": str(e)}
        
        return stats


def main():
    """Point d'entrée"""
    import sys
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from config import Config
    
    print("🚀 Démarrage Module 2 : Auto Loader (Unity Catalog)")
    
    spark = SparkSession.builder.appName("WAX-Module2-AutoLoader").getOrCreate()
    
    config = Config(
        catalog="abu_catalog",
        schema_files="databricksassetbundletest",
        volume="externalvolumetes",
        schema_tables="gdp_poc_dev",
        env="dev",
        version="v1"
    )
    
    excel_path = f"{config.volume_base}/input/config/wax_config.xlsx"
    
    autoloader = AutoLoaderModule(spark, config)
    result = autoloader.process_all_tables(excel_path)
    
    if result["status"] in ["SUCCESS", "PARTIAL"]:
        print("\n📋 Tables staging créées :")
        stats = autoloader.get_staging_stats()
        for table_name, table_stats in stats.items():
            if "error" not in table_stats:
                print(f"   • {table_name}: {table_stats['rows']:,} lignes")
                if table_stats.get('sources'):
                    print(f"     Sources: {', '.join(table_stats['sources'][:3])}")
    
    if result["status"] == "SUCCESS":
        print("\n✅ Module 2 terminé avec succès")
        return 0
    elif result["status"] == "PARTIAL":
        print("\n⚠️  Module 2 terminé avec des erreurs partielles")
        return 1
    else:
        print(f"\n❌ Module 2 terminé avec erreurs")
        return 2


if __name__ == "__main__":
    import sys
    sys.exit(main())

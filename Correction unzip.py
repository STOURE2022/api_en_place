"""
file_handler.py
MODULE 1 : Gestion des fichiers d'entrée (ZIP et fichiers directs)
Extrait les fichiers ZIP et copie les fichiers directs depuis input/ vers extracted/

✨ FEATURES R1:
- Gestion fichiers ZIP
- Gestion fichiers non-ZIP (CSV, Parquet, JSON)
- Suppression automatique des ZIP après extraction
- Organisation par table
"""

import os
import zipfile
import shutil
from datetime import datetime
from pyspark.sql import SparkSession


class FileHandler:
    """Gestionnaire de fichiers d'entrée (ZIP et fichiers directs)"""
    
    # Formats de fichiers supportés
    SUPPORTED_FORMATS = {
        'csv': ['.csv', '.txt'],
        'parquet': ['.parquet'],
        'json': ['.json', '.jsonl']
    }
    
    def __init__(self, spark, config):
        self.spark = spark
        self.config = config
        
        # Chemins Unity Catalog
        self.input_base = f"{config.volume_base}/input"
        self.zip_dir = f"{self.input_base}/zip"
        self.direct_dir = f"{self.input_base}/direct"  # Nouveau: fichiers directs
        self.extract_base_dir = f"{config.volume_base}/extracted"
        self.processed_dir = f"{config.volume_base}/processed"
        
        # Statistiques
        self.stats = {
            'zip_processed': 0,
            'zip_failed': 0,
            'direct_processed': 0,
            'direct_failed': 0,
            'total_files_extracted': 0,
            'zip_deleted': 0
        }
    
    def process_all_files(self) -> dict:
        """
        Traite tous les fichiers d'entrée (ZIP + fichiers directs)
        
        Returns:
            dict: Statistiques du traitement
        """
        
        print("=" * 80)
        print("📦 MODULE 1 : GESTION FICHIERS D'ENTRÉE")
        print("=" * 80)
        
        # Créer répertoires si nécessaire
        self._ensure_directories()
        
        results = []
        
        # 1. Traiter les fichiers ZIP
        print("\n📦 Phase 1: Traitement des fichiers ZIP")
        print("─" * 80)
        zip_results = self._process_zip_files()
        results.extend(zip_results)
        
        # 2. Traiter les fichiers directs
        print("\n📄 Phase 2: Traitement des fichiers directs")
        print("─" * 80)
        direct_results = self._process_direct_files()
        results.extend(direct_results)
        
        # Résumé final
        self._print_summary()
        
        return {
            "status": self._get_overall_status(),
            "stats": self.stats,
            "results": results
        }
    
    def _ensure_directories(self):
        """Crée tous les répertoires nécessaires"""
        directories = [
            self.input_base,
            self.zip_dir,
            self.direct_dir,
            self.extract_base_dir,
            self.processed_dir
        ]
        
        for directory in directories:
            os.makedirs(directory, exist_ok=True)
    
    # =========================================================================
    # GESTION DES FICHIERS ZIP
    # =========================================================================
    
    def _process_zip_files(self) -> list:
        """Traite tous les fichiers ZIP"""
        
        print(f"📂 Répertoire source : {self.zip_dir}")
        print(f"📂 Répertoire cible  : {self.extract_base_dir}")
        
        results = []
        
        try:
            if not os.path.exists(self.zip_dir):
                print(f"⚠️  Répertoire ZIP introuvable : {self.zip_dir}")
                print(f"   Créer le répertoire et y placer les fichiers ZIP")
                return results
            
            all_files = os.listdir(self.zip_dir)
            zip_files = [f for f in all_files if f.endswith('.zip')]
            
        except Exception as e:
            print(f"❌ Erreur listage ZIP : {e}")
            return results
        
        if not zip_files:
            print("ℹ️  Aucun fichier ZIP trouvé")
            return results
        
        print(f"✅ {len(zip_files)} fichier(s) ZIP trouvé(s)")
        
        # Extraire chaque ZIP
        for idx, zip_file in enumerate(zip_files, 1):
            print(f"\n{'─' * 80}")
            print(f"📦 ZIP {idx}/{len(zip_files)}: {zip_file}")
            print(f"{'─' * 80}")
            
            try:
                result = self._extract_single_zip(zip_file)
                
                if result["status"] == "SUCCESS":
                    self.stats['zip_processed'] += 1
                    self.stats['total_files_extracted'] += result["file_count"]
                    print(f"✅ {result['file_count']} fichier(s) extrait(s)")
                    print(f"   → {result['extract_dir']}")
                    
                    # Supprimer le ZIP après extraction réussie
                    if self._delete_zip(zip_file):
                        print(f"🗑️  ZIP supprimé")
                        self.stats['zip_deleted'] += 1
                    
                    results.append({
                        "file_name": zip_file,
                        "file_type": "ZIP",
                        "status": "SUCCESS",
                        "file_count": result["file_count"],
                        "target_table": result.get("target_table", "unknown"),
                        "zip_deleted": True
                    })
                else:
                    self.stats['zip_failed'] += 1
                    print(f"❌ Échec : {result.get('error', 'Unknown')}")
                    
                    results.append({
                        "file_name": zip_file,
                        "file_type": "ZIP",
                        "status": "FAILED",
                        "error": result.get("error", "Unknown")
                    })
                    
            except Exception as e:
                self.stats['zip_failed'] += 1
                print(f"❌ Erreur : {e}")
                import traceback
                traceback.print_exc()
                
                results.append({
                    "file_name": zip_file,
                    "file_type": "ZIP",
                    "status": "FAILED",
                    "error": str(e)
                })
        
        return results
    
    def _extract_single_zip(self, zip_filename: str) -> dict:
        """
        Extrait un seul fichier ZIP
        
        Args:
            zip_filename: Nom du fichier ZIP
            
        Returns:
            dict: Résultat de l'extraction
        """
        
        zip_path = os.path.join(self.zip_dir, zip_filename)
        
        # Déterminer le nom de la table
        table_name = self._extract_table_name(zip_filename)
        extract_dir = os.path.join(self.extract_base_dir, table_name)
        
        # Créer répertoire si nécessaire
        os.makedirs(extract_dir, exist_ok=True)
        
        try:
            # Extraire
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                # Lister les fichiers dans le ZIP
                file_list = zip_ref.namelist()
                
                # Filtrer les fichiers système et dossiers
                valid_files = [f for f in file_list 
                              if not f.startswith('__MACOSX') 
                              and not f.startswith('.')
                              and not f.endswith('/')]
                
                if not valid_files:
                    return {
                        "status": "ERROR",
                        "error": "No valid files in ZIP"
                    }
                
                # Extraire seulement les fichiers valides
                for file in valid_files:
                    zip_ref.extract(file, extract_dir)
                
                file_count = len(valid_files)
            
            return {
                "status": "SUCCESS",
                "file_count": file_count,
                "extract_dir": extract_dir,
                "target_table": table_name
            }
            
        except zipfile.BadZipFile as e:
            return {
                "status": "ERROR",
                "error": f"Invalid ZIP file: {e}"
            }
        except Exception as e:
            return {
                "status": "ERROR",
                "error": str(e)
            }
    
    def _delete_zip(self, zip_filename: str) -> bool:
        """
        Supprime un fichier ZIP après extraction réussie
        
        Args:
            zip_filename: Nom du fichier ZIP
            
        Returns:
            bool: True si supprimé avec succès
        """
        try:
            zip_path = os.path.join(self.zip_dir, zip_filename)
            
            # Option 1: Supprimer directement
            os.remove(zip_path)
            return True
            
            # Option 2 (alternative): Déplacer vers processed/ avant de supprimer
            # processed_path = os.path.join(self.processed_dir, zip_filename)
            # shutil.move(zip_path, processed_path)
            # return True
            
        except Exception as e:
            print(f"⚠️  Impossible de supprimer {zip_filename}: {e}")
            return False
    
    # =========================================================================
    # GESTION DES FICHIERS DIRECTS (CSV, Parquet, JSON)
    # =========================================================================
    
    def _process_direct_files(self) -> list:
        """
        Traite les fichiers directs (non-ZIP)
        
        Returns:
            list: Résultats du traitement
        """
        
        print(f"📂 Répertoire source : {self.direct_dir}")
        print(f"📂 Répertoire cible  : {self.extract_base_dir}")
        
        results = []
        
        try:
            if not os.path.exists(self.direct_dir):
                print(f"⚠️  Répertoire introuvable : {self.direct_dir}")
                return results
            
            all_files = os.listdir(self.direct_dir)
            
            # Filtrer les fichiers supportés
            direct_files = [f for f in all_files 
                           if self._is_supported_format(f) and os.path.isfile(os.path.join(self.direct_dir, f))]
            
        except Exception as e:
            print(f"❌ Erreur listage fichiers directs : {e}")
            return results
        
        if not direct_files:
            print("ℹ️  Aucun fichier direct trouvé")
            return results
        
        print(f"✅ {len(direct_files)} fichier(s) direct(s) trouvé(s)")
        
        # Traiter chaque fichier
        for idx, filename in enumerate(direct_files, 1):
            print(f"\n{'─' * 80}")
            print(f"📄 Fichier {idx}/{len(direct_files)}: {filename}")
            print(f"{'─' * 80}")
            
            try:
                result = self._copy_direct_file(filename)
                
                if result["status"] == "SUCCESS":
                    self.stats['direct_processed'] += 1
                    self.stats['total_files_extracted'] += 1
                    print(f"✅ Fichier copié")
                    print(f"   → {result['target_path']}")
                    
                    results.append({
                        "file_name": filename,
                        "file_type": result["file_format"].upper(),
                        "status": "SUCCESS",
                        "target_table": result["target_table"],
                        "target_path": result["target_path"]
                    })
                else:
                    self.stats['direct_failed'] += 1
                    print(f"❌ Échec : {result.get('error', 'Unknown')}")
                    
                    results.append({
                        "file_name": filename,
                        "file_type": "UNKNOWN",
                        "status": "FAILED",
                        "error": result.get("error", "Unknown")
                    })
                    
            except Exception as e:
                self.stats['direct_failed'] += 1
                print(f"❌ Erreur : {e}")
                
                results.append({
                    "file_name": filename,
                    "file_type": "UNKNOWN",
                    "status": "FAILED",
                    "error": str(e)
                })
        
        return results
    
    def _copy_direct_file(self, filename: str) -> dict:
        """
        Copie un fichier direct vers le répertoire d'extraction
        
        Args:
            filename: Nom du fichier
            
        Returns:
            dict: Résultat de la copie
        """
        
        source_path = os.path.join(self.direct_dir, filename)
        
        # Déterminer format et table
        file_format = self._get_file_format(filename)
        table_name = self._extract_table_name(filename)
        
        # Créer répertoire cible
        target_dir = os.path.join(self.extract_base_dir, table_name)
        os.makedirs(target_dir, exist_ok=True)
        
        target_path = os.path.join(target_dir, filename)
        
        try:
            # Copier le fichier
            shutil.copy2(source_path, target_path)
            
            # Supprimer le fichier source après copie réussie
            os.remove(source_path)
            
            return {
                "status": "SUCCESS",
                "file_format": file_format,
                "target_table": table_name,
                "target_path": target_path
            }
            
        except Exception as e:
            return {
                "status": "ERROR",
                "error": str(e)
            }
    
    # =========================================================================
    # UTILITAIRES
    # =========================================================================
    
    def _is_supported_format(self, filename: str) -> bool:
        """Vérifie si le format du fichier est supporté"""
        ext = os.path.splitext(filename.lower())[1]
        
        for format_type, extensions in self.SUPPORTED_FORMATS.items():
            if ext in extensions:
                return True
        return False
    
    def _get_file_format(self, filename: str) -> str:
        """Détermine le format d'un fichier"""
        ext = os.path.splitext(filename.lower())[1]
        
        for format_type, extensions in self.SUPPORTED_FORMATS.items():
            if ext in extensions:
                return format_type
        
        return 'unknown'
    
    def _extract_table_name(self, filename: str) -> str:
        """
        Extrait le nom de la table depuis le nom du fichier
        
        Examples:
            site_20250902.zip → site
            client.csv → client
            orders_2025.parquet → orders
        """
        # Retirer l'extension
        base_name = filename
        for extensions in self.SUPPORTED_FORMATS.values():
            for ext in extensions:
                if base_name.lower().endswith(ext):
                    base_name = base_name[:-len(ext)]
                    break
        
        # Retirer .zip si présent
        if base_name.endswith('.zip'):
            base_name = base_name[:-4]
        
        # Extraire la première partie avant underscore
        if '_' in base_name:
            table_name = base_name.split('_')[0]
        else:
            table_name = base_name
        
        return table_name.lower()
    
    def list_extracted_files(self, table_name: str = None) -> dict:
        """
        Liste les fichiers extraits
        
        Args:
            table_name: Nom de la table (optionnel)
            
        Returns:
            dict: Fichiers extraits par table
        """
        
        try:
            if table_name:
                target_dir = os.path.join(self.extract_base_dir, table_name)
                if os.path.exists(target_dir):
                    files = os.listdir(target_dir)
                    return {table_name: files}
                return {}
            else:
                # Lister toutes les tables
                all_files = {}
                if os.path.exists(self.extract_base_dir):
                    for table_dir in os.listdir(self.extract_base_dir):
                        table_path = os.path.join(self.extract_base_dir, table_dir)
                        if os.path.isdir(table_path):
                            all_files[table_dir] = os.listdir(table_path)
                return all_files
                
        except Exception as e:
            print(f"⚠️  Erreur listage fichiers extraits : {e}")
            return {}
    
    def _get_overall_status(self) -> str:
        """Détermine le statut global du traitement"""
        total_failed = self.stats['zip_failed'] + self.stats['direct_failed']
        total_processed = self.stats['zip_processed'] + self.stats['direct_processed']
        
        if total_processed == 0:
            return "NO_DATA"
        elif total_failed == 0:
            return "SUCCESS"
        elif total_failed < total_processed:
            return "PARTIAL"
        else:
            return "ERROR"
    
    def _print_summary(self):
        """Affiche le résumé du traitement"""
        print("\n" + "=" * 80)
        print("📊 RÉSUMÉ TRAITEMENT FICHIERS")
        print("=" * 80)
        
        print("\n📦 Fichiers ZIP:")
        print(f"   ✅ Extraits       : {self.stats['zip_processed']}")
        print(f"   🗑️  Supprimés      : {self.stats['zip_deleted']}")
        print(f"   ❌ Échecs         : {self.stats['zip_failed']}")
        
        print("\n📄 Fichiers directs:")
        print(f"   ✅ Copiés         : {self.stats['direct_processed']}")
        print(f"   ❌ Échecs         : {self.stats['direct_failed']}")
        
        print("\n📊 Total:")
        print(f"   📄 Fichiers traités : {self.stats['total_files_extracted']}")
        
        print("=" * 80)


def main():
    """Point d'entrée du module"""
    
    import sys
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    
    from config import Config
    
    print("🚀 Démarrage Module 1 : Gestion Fichiers")
    
    # Initialiser Spark
    spark = SparkSession.builder.appName("WAX-Module1-FileHandler").getOrCreate()
    
    # Configuration
    config = Config(
        catalog="abu_catalog",
        schema_files="databricksassetbundletest",
        volume="externalvolumetes",
        schema_tables="gdp_poc_dev",
        env="dev",
        version="v1"
    )
    
    # Traiter tous les fichiers
    file_handler = FileHandler(spark, config)
    result = file_handler.process_all_files()
    
    # Afficher fichiers extraits
    if result["status"] in ["SUCCESS", "PARTIAL"]:
        print("\n📋 Fichiers extraits par table :")
        extracted = file_handler.list_extracted_files()
        for table, files in extracted.items():
            print(f"\n   📊 Table: {table}")
            print(f"      Fichiers: {len(files)}")
            for f in files[:5]:
                file_format = file_handler._get_file_format(f)
                print(f"      • {f} [{file_format.upper()}]")
            if len(files) > 5:
                print(f"      ... et {len(files) - 5} autre(s)")
    
    # Retourner code de sortie
    if result["status"] == "SUCCESS":
        print("\n✅ Module 1 terminé avec succès")
        return 0
    elif result["status"] == "PARTIAL":
        print("\n⚠️  Module 1 terminé avec des erreurs partielles")
        return 1
    else:
        print(f"\n❌ Module 1 terminé avec erreurs")
        return 2


if __name__ == "__main__":
    import sys
    sys.exit(main())

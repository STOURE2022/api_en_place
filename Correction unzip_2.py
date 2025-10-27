"""
unzip_module.py
MODULE 1 : Dézipage des fichiers ZIP et gestion fichiers directs
Extrait les fichiers ZIP et copie les fichiers directs depuis input/zip/ vers extracted/

✨ FEATURES R1:
- Gestion fichiers ZIP avec extraction
- Gestion fichiers non-ZIP directs (CSV, Parquet, JSON)
- Suppression automatique après traitement
- Un seul point de dépôt: input/zip/
"""

import os
import zipfile
import shutil
from datetime import datetime
from pyspark.sql import SparkSession


class UnzipManager:
    """Gestionnaire de dézipage de fichiers ZIP et fichiers directs"""
    
    # Formats de fichiers supportés (en plus des ZIP)
    SUPPORTED_FORMATS = {
        'csv': ['.csv', '.txt'],
        'parquet': ['.parquet'],
        'json': ['.json', '.jsonl']
    }
    
    def __init__(self, spark, config):
        self.spark = spark
        self.config = config
        
        # Chemins Unity Catalog (directs - fonctionne dans Databricks Jobs)
        self.zip_dir = f"{config.volume_base}/input/zip"
        self.extract_base_dir = f"{config.volume_base}/extracted"
        
        # Statistiques
        self.stats = {
            'zip_processed': 0,
            'zip_failed': 0,
            'direct_processed': 0,
            'direct_failed': 0,
            'total_files_extracted': 0,
            'files_deleted': 0
        }
    
    def process_all_zips(self) -> dict:
        """
        Traite tous les fichiers présents dans input/zip/
        Gère à la fois les ZIP et les fichiers directs (CSV, Parquet, JSON)
        
        Returns:
            dict: Statistiques du traitement
        """
        
        print("=" * 80)
        print("📦 MODULE 1 : DÉZIPAGE ET GESTION FICHIERS")
        print("=" * 80)
        
        print(f"\n📂 Répertoire source : {self.zip_dir}")
        print(f"📂 Répertoire cible  : {self.extract_base_dir}")
        
        # Créer répertoires si nécessaire
        os.makedirs(self.extract_base_dir, exist_ok=True)
        
        # Lister tous les fichiers
        try:
            if not os.path.exists(self.zip_dir):
                print(f"\n⚠️  Répertoire introuvable : {self.zip_dir}")
                print(f"   Créer le répertoire et y placer les fichiers")
                os.makedirs(self.zip_dir, exist_ok=True)
                return {"status": "NO_DATA", "zip_count": 0, "error": "Directory created"}
            
            all_files = os.listdir(self.zip_dir)
            
            # Séparer ZIP et fichiers directs
            zip_files = [f for f in all_files if f.endswith('.zip')]
            direct_files = [f for f in all_files if self._is_supported_format(f) and os.path.isfile(os.path.join(self.zip_dir, f))]
            
        except Exception as e:
            print(f"❌ Erreur listage fichiers : {e}")
            return {"status": "ERROR", "error": str(e), "zip_count": 0}
        
        if not zip_files and not direct_files:
            print("\n⚠️  Aucun fichier trouvé dans input/zip/")
            return {"status": "NO_DATA", "zip_count": 0}
        
        print(f"\n✅ {len(zip_files)} fichier(s) ZIP trouvé(s)")
        print(f"✅ {len(direct_files)} fichier(s) direct(s) trouvé(s) (CSV/Parquet/JSON)")
        
        results = []
        
        # Phase 1: Traiter les fichiers ZIP
        if zip_files:
            print(f"\n{'─' * 80}")
            print("📦 PHASE 1: TRAITEMENT DES FICHIERS ZIP")
            print(f"{'─' * 80}")
            
            for idx, zip_file in enumerate(zip_files, 1):
                print(f"\n📦 ZIP {idx}/{len(zip_files)}: {zip_file}")
                
                try:
                    result = self._extract_single_zip(zip_file)
                    
                    if result["status"] == "SUCCESS":
                        self.stats['zip_processed'] += 1
                        self.stats['total_files_extracted'] += result["file_count"]
                        print(f"✅ {result['file_count']} fichier(s) extrait(s)")
                        print(f"   → {result['extract_dir']}")
                        
                        # Supprimer le ZIP après extraction réussie
                        if self._delete_file(zip_file):
                            print(f"🗑️  ZIP supprimé")
                            self.stats['files_deleted'] += 1
                        
                        results.append({
                            "file_name": zip_file,
                            "file_type": "ZIP",
                            "status": "SUCCESS",
                            "file_count": result["file_count"],
                            "target_table": result.get("target_table", "unknown")
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
        
        # Phase 2: Traiter les fichiers directs
        if direct_files:
            print(f"\n{'─' * 80}")
            print("📄 PHASE 2: TRAITEMENT DES FICHIERS DIRECTS")
            print(f"{'─' * 80}")
            
            for idx, filename in enumerate(direct_files, 1):
                file_format = self._get_file_format(filename)
                print(f"\n📄 Fichier {idx}/{len(direct_files)}: {filename} [{file_format.upper()}]")
                
                try:
                    result = self._copy_direct_file(filename)
                    
                    if result["status"] == "SUCCESS":
                        self.stats['direct_processed'] += 1
                        self.stats['total_files_extracted'] += 1
                        print(f"✅ Fichier copié")
                        print(f"   → {result['target_path']}")
                        
                        # Supprimer le fichier après copie réussie
                        if self._delete_file(filename):
                            print(f"🗑️  Fichier supprimé")
                            self.stats['files_deleted'] += 1
                        
                        results.append({
                            "file_name": filename,
                            "file_type": result["file_format"].upper(),
                            "status": "SUCCESS",
                            "target_table": result["target_table"]
                        })
                    else:
                        self.stats['direct_failed'] += 1
                        print(f"❌ Échec : {result.get('error', 'Unknown')}")
                        
                        results.append({
                            "file_name": filename,
                            "file_type": file_format.upper(),
                            "status": "FAILED",
                            "error": result.get("error", "Unknown")
                        })
                        
                except Exception as e:
                    self.stats['direct_failed'] += 1
                    print(f"❌ Erreur : {e}")
                    
                    results.append({
                        "file_name": filename,
                        "file_type": file_format.upper(),
                        "status": "FAILED",
                        "error": str(e)
                    })
        
        # Résumé
        self._print_summary()
        
        # Calcul du zip_count pour compatibilité
        zip_count = self.stats['zip_processed'] + self.stats['direct_processed']
        failed_count = self.stats['zip_failed'] + self.stats['direct_failed']
        
        return {
            "status": "SUCCESS" if failed_count == 0 else "PARTIAL" if zip_count > 0 else "ERROR",
            "zip_count": zip_count,  # Total fichiers traités (pour compatibilité)
            "failed_count": failed_count,
            "total_files": self.stats['total_files_extracted'],
            "stats": self.stats,
            "results": results
        }
    
    def _extract_single_zip(self, zip_filename: str) -> dict:
        """
        Extrait un seul fichier ZIP
        
        Args:
            zip_filename: Nom du fichier ZIP
            
        Returns:
            dict: Résultat de l'extraction
        """
        
        zip_path = os.path.join(self.zip_dir, zip_filename)
        
        # Déterminer le répertoire d'extraction selon le nom du fichier
        # Ex: site_20250902.zip → extracted/site/
        base_name = zip_filename.replace('.zip', '')
        
        # Extraire le nom de la table (première partie avant _)
        if '_' in base_name:
            table_name = base_name.split('_')[0]
        else:
            table_name = base_name
        
        extract_dir = os.path.join(self.extract_base_dir, table_name)
        
        # Créer répertoire si nécessaire
        os.makedirs(extract_dir, exist_ok=True)
        
        try:
            # Extraire
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                # Lister les fichiers dans le ZIP
                file_list = zip_ref.namelist()
                
                # Filtrer les fichiers système
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
    
    def _copy_direct_file(self, filename: str) -> dict:
        """
        Copie un fichier direct vers le répertoire d'extraction
        
        Args:
            filename: Nom du fichier
            
        Returns:
            dict: Résultat de la copie
        """
        
        source_path = os.path.join(self.zip_dir, filename)
        
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
    
    def _delete_file(self, filename: str) -> bool:
        """
        Supprime un fichier après traitement réussi
        
        Args:
            filename: Nom du fichier
            
        Returns:
            bool: True si supprimé avec succès
        """
        try:
            file_path = os.path.join(self.zip_dir, filename)
            os.remove(file_path)
            return True
        except Exception as e:
            print(f"⚠️  Impossible de supprimer {filename}: {e}")
            return False
    
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
            site_20250902.csv → site
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
        
        # Extraire la première partie avant underscore
        if '_' in base_name:
            table_name = base_name.split('_')[0]
        else:
            table_name = base_name
        
        return table_name.lower()
    
    def _print_summary(self):
        """Affiche le résumé du traitement"""
        print("\n" + "=" * 80)
        print("📊 RÉSUMÉ TRAITEMENT")
        print("=" * 80)
        
        print("\n📦 Fichiers ZIP:")
        print(f"   ✅ Extraits       : {self.stats['zip_processed']}")
        print(f"   ❌ Échecs         : {self.stats['zip_failed']}")
        
        print("\n📄 Fichiers directs:")
        print(f"   ✅ Copiés         : {self.stats['direct_processed']}")
        print(f"   ❌ Échecs         : {self.stats['direct_failed']}")
        
        print("\n📊 Total:")
        print(f"   📄 Fichiers traités : {self.stats['total_files_extracted']}")
        print(f"   🗑️  Fichiers supprimés : {self.stats['files_deleted']}")
        
        print("=" * 80)
    
    def list_extracted_files(self, table_name: str = None) -> list:
        """
        Liste les fichiers extraits
        
        Args:
            table_name: Nom de la table (optionnel)
            
        Returns:
            Liste des fichiers extraits (pour compatibilité avec l'ancien code)
        """
        
        try:
            if table_name:
                target_dir = os.path.join(self.extract_base_dir, table_name)
                if os.path.exists(target_dir):
                    return os.listdir(target_dir)
                return []
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
            return [] if table_name else {}


def main():
    """Point d'entrée du module dézipage"""
    
    import sys
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    
    from config import Config
    
    print("🚀 Démarrage Module 1 : Dézipage et Gestion Fichiers")
    
    # Initialiser Spark
    spark = SparkSession.builder.appName("WAX-Module1-Unzip").getOrCreate()
    
    # Configuration
    config = Config(
        catalog="abu_catalog",
        schema_files="databricksassetbundletest",
        volume="externalvolumetes",
        schema_tables="gdp_poc_dev",
        env="dev",
        version="v1"
    )
    
    # Dézipper et traiter tous les fichiers
    unzip_manager = UnzipManager(spark, config)
    result = unzip_manager.process_all_zips()
    
    # Afficher fichiers extraits
    if result["status"] in ["SUCCESS", "PARTIAL"]:
        print("\n📋 Fichiers extraits par table :")
        extracted = unzip_manager.list_extracted_files()
        if isinstance(extracted, dict):
            for table, files in extracted.items():
                print(f"   • {table}: {len(files)} fichier(s)")
                for f in files[:3]:
                    # Afficher le format du fichier
                    file_format = unzip_manager._get_file_format(f)
                    print(f"      - {f} [{file_format.upper()}]")
                if len(files) > 3:
                    print(f"      ... et {len(files) - 3} autre(s)")
    
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

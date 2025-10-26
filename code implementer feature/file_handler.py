"""
file_handler.py
Gestionnaire de fichiers non-ZIP
Gère les fichiers CSV/Parquet/JSON directs sans extraction ZIP
"""

import os
import shutil
from datetime import datetime
from pyspark.sql import SparkSession


class FileHandler:
    """
    Gestionnaire pour fichiers non-ZIP
    
    Features:
    - Détection type de fichier (ZIP vs non-ZIP)
    - Copie fichiers directs vers /extracted/
    - Organisation par table
    """
    
    def __init__(self, spark: SparkSession, config):
        self.spark = spark
        self.config = config
        
        # Chemins
        self.input_dir = f"{config.volume_base}/input/zip"  # Source (même dossier que ZIP)
        self.extract_base_dir = f"{config.volume_base}/extracted"
        
        # Conversion pour Python standard
        self.input_dir_fs = self._to_fs_path(self.input_dir)
        self.extract_base_dir_fs = self._to_fs_path(self.extract_base_dir)
        
        # Formats supportés
        self.supported_formats = ['.csv', '.parquet', '.json', '.txt']
    
    def _to_fs_path(self, unity_catalog_path: str) -> str:
        """Convertit chemin Unity Catalog en chemin système"""
        if unity_catalog_path.startswith("/Volumes"):
            return unity_catalog_path.replace("/Volumes", "/dbfs/Volumes")
        return unity_catalog_path
    
    def is_zip_file(self, filename: str) -> bool:
        """Vérifie si le fichier est un ZIP"""
        return filename.lower().endswith('.zip')
    
    def is_supported_format(self, filename: str) -> bool:
        """Vérifie si le format est supporté"""
        ext = os.path.splitext(filename)[1].lower()
        return ext in self.supported_formats
    
    def get_table_name_from_filename(self, filename: str) -> str:
        """
        Extrait le nom de la table depuis le nom de fichier
        
        Exemples:
        - customers_20251016.csv → customers
        - site_data.parquet → site
        - billing_20251016_101112.csv → billing
        """
        
        # Enlever extension
        base_name = os.path.splitext(filename)[0]
        
        # Extraire première partie avant _
        if '_' in base_name:
            return base_name.split('_')[0]
        
        return base_name
    
    def process_non_zip_files(self) -> dict:
        """
        Traite tous les fichiers non-ZIP présents dans input/
        
        Returns:
            dict: Statistiques du traitement
        """
        
        print("=" * 80)
        print("📄 TRAITEMENT FICHIERS NON-ZIP")
        print("=" * 80)
        
        print(f"\n📂 Répertoire source : {self.input_dir}")
        print(f"📂 Répertoire cible  : {self.extract_base_dir}")
        
        # Créer répertoire extraction si nécessaire
        os.makedirs(self.extract_base_dir_fs, exist_ok=True)
        
        # Lister tous les fichiers
        try:
            if not os.path.exists(self.input_dir_fs):
                print(f"\n⚠️  Répertoire input introuvable : {self.input_dir}")
                return {"status": "NO_DATA", "file_count": 0}
            
            all_files = os.listdir(self.input_dir_fs)
            
            # Filtrer fichiers non-ZIP et formats supportés
            non_zip_files = [
                f for f in all_files 
                if not self.is_zip_file(f) and self.is_supported_format(f)
            ]
            
        except Exception as e:
            print(f"❌ Erreur listage fichiers : {e}")
            return {"status": "ERROR", "error": str(e), "file_count": 0}
        
        if not non_zip_files:
            print("\n✅ Aucun fichier non-ZIP à traiter")
            return {"status": "NO_DATA", "file_count": 0}
        
        print(f"\n✅ {len(non_zip_files)} fichier(s) non-ZIP trouvé(s)")
        
        # Traiter chaque fichier
        processed_count = 0
        failed_count = 0
        results = []
        
        for idx, filename in enumerate(non_zip_files, 1):
            print(f"\n{'─' * 80}")
            print(f"📄 Fichier {idx}/{len(non_zip_files)}: {filename}")
            print(f"{'─' * 80}")
            
            try:
                result = self._copy_file_to_extracted(filename)
                
                if result["status"] == "SUCCESS":
                    processed_count += 1
                    print(f"✅ Copié vers : {result['dest_path']}")
                    
                    results.append({
                        "filename": filename,
                        "status": "SUCCESS",
                        "table_name": result["table_name"],
                        "dest_path": result["dest_path"]
                    })
                else:
                    failed_count += 1
                    print(f"❌ Échec : {result.get('error', 'Unknown')}")
                    
                    results.append({
                        "filename": filename,
                        "status": "FAILED",
                        "error": result.get("error", "Unknown")
                    })
            
            except Exception as e:
                failed_count += 1
                print(f"❌ Erreur : {e}")
                
                results.append({
                    "filename": filename,
                    "status": "FAILED",
                    "error": str(e)
                })
        
        # Résumé
        print("\n" + "=" * 80)
        print("📊 RÉSUMÉ FICHIERS NON-ZIP")
        print("=" * 80)
        print(f"✅ Fichiers copiés   : {processed_count}")
        print(f"❌ Fichiers en échec : {failed_count}")
        print("=" * 80)
        
        return {
            "status": "SUCCESS" if failed_count == 0 else "PARTIAL",
            "file_count": processed_count,
            "failed_count": failed_count,
            "results": results
        }
    
    def _copy_file_to_extracted(self, filename: str) -> dict:
        """
        Copie un fichier vers le répertoire extracted/
        
        Args:
            filename: Nom du fichier à copier
        
        Returns:
            dict: Résultat de la copie
        """
        
        source_path = os.path.join(self.input_dir_fs, filename)
        
        # Déterminer table cible
        table_name = self.get_table_name_from_filename(filename)
        
        # Créer répertoire cible
        target_dir = os.path.join(self.extract_base_dir_fs, table_name)
        os.makedirs(target_dir, exist_ok=True)
        
        # Chemin destination
        dest_path = os.path.join(target_dir, filename)
        
        try:
            # Copier fichier
            shutil.copy2(source_path, dest_path)
            
            # Vérifier copie
            if os.path.exists(dest_path):
                file_size = os.path.getsize(dest_path)
                
                return {
                    "status": "SUCCESS",
                    "table_name": table_name,
                    "dest_path": dest_path.replace("/dbfs", ""),  # Chemin Unity Catalog
                    "file_size": file_size
                }
            else:
                return {
                    "status": "ERROR",
                    "error": "File not found after copy"
                }
        
        except Exception as e:
            return {
                "status": "ERROR",
                "error": str(e)
            }
    
    def move_processed_file(self, filename: str, delete: bool = False):
        """
        Déplace ou supprime le fichier source après traitement
        
        Args:
            filename: Nom du fichier
            delete: Si True, supprime. Si False, déplace vers /processed/
        """
        
        source_path = os.path.join(self.input_dir_fs, filename)
        
        try:
            if delete:
                # Supprimer
                os.remove(source_path)
                print(f"   🗑️  Supprimé : {filename}")
            else:
                # Déplacer vers /processed/
                processed_dir = os.path.join(self.input_dir_fs, "processed")
                os.makedirs(processed_dir, exist_ok=True)
                
                dest_path = os.path.join(processed_dir, filename)
                shutil.move(source_path, dest_path)
                print(f"   📦 Archivé : {filename}")
        
        except Exception as e:
            print(f"   ⚠️  Erreur gestion fichier source : {e}")
    
    def list_non_zip_files(self) -> list:
        """
        Liste tous les fichiers non-ZIP dans input/
        
        Returns:
            Liste des fichiers
        """
        
        try:
            if not os.path.exists(self.input_dir_fs):
                return []
            
            all_files = os.listdir(self.input_dir_fs)
            
            return [
                f for f in all_files 
                if not self.is_zip_file(f) and self.is_supported_format(f)
            ]
        
        except Exception as e:
            print(f"⚠️  Erreur listage fichiers : {e}")
            return []


def main():
    """Test du file handler"""
    
    import sys
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    
    from config import Config
    
    print("🚀 Test File Handler - Fichiers Non-ZIP")
    
    # Initialiser Spark
    spark = SparkSession.builder.appName("WAX-FileHandlerTest").getOrCreate()
    
    # Configuration
    config = Config(
        catalog="abu_catalog",
        schema_files="databricksassetbundletest",
        volume="externalvolumetes",
        schema_tables="gdp_poc_dev",
        env="dev"
    )
    
    # Créer file handler
    handler = FileHandler(spark, config)
    
    # Lister fichiers
    print("\n📋 Fichiers non-ZIP disponibles :")
    files = handler.list_non_zip_files()
    for f in files:
        print(f"   • {f}")
    
    # Traiter fichiers
    if files:
        result = handler.process_non_zip_files()
        
        print(f"\n📊 Résultat final : {result['status']}")
        print(f"   Fichiers traités : {result['file_count']}")
    else:
        print("\n✅ Aucun fichier à traiter")


if __name__ == "__main__":
    main()

"""
tracking_manager.py
Gestionnaire de tracking des fichiers traités
- Prévention des duplicates
- Validation ordre chronologique des fichiers
"""

from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as spark_max, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
import re


class TrackingManager:
    """
    Gestionnaire pour tracker les fichiers traités et valider l'ordre
    
    Features:
    - Prévention traitement fichiers dupliqués (ERROR 000000002)
    - Validation ordre chronologique (ERROR 000000003)
    """
    
    def __init__(self, spark: SparkSession, config):
        self.spark = spark
        self.config = config
        self.tracking_table = f"{config.catalog}.{config.schema_tables}.wax_processed_files"
        
        # Créer la table de tracking si elle n'existe pas
        self._create_tracking_table()
    
    def _create_tracking_table(self):
        """
        Crée la table de tracking des fichiers traités
        
        Colonnes:
        - table_name: Nom de la table cible
        - filename: Nom du fichier traité
        - file_date: Date extraite du filename
        - processed_date: Date de traitement
        - status: SUCCESS ou FAILED
        """
        
        schema = StructType([
            StructField("table_name", StringType(), False),
            StructField("filename", StringType(), False),
            StructField("file_date", TimestampType(), True),
            StructField("processed_date", TimestampType(), False),
            StructField("status", StringType(), False),
            StructField("row_count", StringType(), True)
        ])
        
        try:
            # Vérifier si table existe
            table_exists = self.spark.catalog.tableExists(self.tracking_table)
            
            if not table_exists:
                print(f"📊 Création table tracking : {self.tracking_table}")
                
                # Créer table vide
                empty_df = self.spark.createDataFrame([], schema)
                empty_df.write.format("delta").saveAsTable(self.tracking_table)
                
                print(f"   ✅ Table créée")
        
        except Exception as e:
            print(f"⚠️  Erreur création table tracking : {e}")
    
    def check_duplicate(self, table_name: str, filename: str) -> dict:
        """
        Vérifie si le fichier a déjà été traité
        
        Args:
            table_name: Nom de la table
            filename: Nom du fichier
        
        Returns:
            dict: {
                "is_duplicate": bool,
                "error_code": "000000002" si duplicate,
                "message": message d'erreur
            }
        """
        
        try:
            # Vérifier si fichier existe dans tracking
            tracking_df = self.spark.table(self.tracking_table)
            
            duplicate = tracking_df.filter(
                (col("table_name") == table_name) &
                (col("filename") == filename) &
                (col("status") == "SUCCESS")
            ).count()
            
            if duplicate > 0:
                return {
                    "is_duplicate": True,
                    "error_code": "000000002",
                    "message": f"File {filename} already processed for table {table_name}"
                }
            
            return {
                "is_duplicate": False,
                "error_code": None,
                "message": None
            }
        
        except Exception as e:
            print(f"⚠️  Erreur vérification duplicate : {e}")
            # En cas d'erreur, on laisse passer pour ne pas bloquer
            return {
                "is_duplicate": False,
                "error_code": None,
                "message": None
            }
    
    def extract_date_from_filename(self, filename: str) -> datetime:
        """
        Extrait la date depuis le nom de fichier
        
        Patterns supportés:
        - filename_yyyymmdd_hhmmss.csv
        - filename_yyyymmdd.csv
        - yyyymmdd_filename.csv
        - filename_yyyy_mm_dd.csv
        
        Args:
            filename: Nom du fichier
        
        Returns:
            datetime ou None si pas trouvé
        """
        
        # Pattern 1: yyyymmdd_hhmmss (ex: 20251016_101112)
        pattern1 = r'(\d{8})_(\d{6})'
        match = re.search(pattern1, filename)
        if match:
            date_str = match.group(1)
            time_str = match.group(2)
            return datetime.strptime(f"{date_str}{time_str}", "%Y%m%d%H%M%S")
        
        # Pattern 2: yyyymmdd (ex: 20251016)
        pattern2 = r'(\d{8})'
        match = re.search(pattern2, filename)
        if match:
            date_str = match.group(1)
            return datetime.strptime(date_str, "%Y%m%d")
        
        # Pattern 3: yyyy-mm-dd (ex: 2025-10-16)
        pattern3 = r'(\d{4})-(\d{2})-(\d{2})'
        match = re.search(pattern3, filename)
        if match:
            return datetime.strptime(f"{match.group(1)}{match.group(2)}{match.group(3)}", "%Y%m%d")
        
        # Pattern 4: yyyy_mm_dd (ex: 2025_10_16)
        pattern4 = r'(\d{4})_(\d{2})_(\d{2})'
        match = re.search(pattern4, filename)
        if match:
            return datetime.strptime(f"{match.group(1)}{match.group(2)}{match.group(3)}", "%Y%m%d")
        
        return None
    
    def check_file_order(self, table_name: str, filename: str) -> dict:
        """
        Vérifie que la date du fichier est >= dernière date traitée
        
        Args:
            table_name: Nom de la table
            filename: Nom du fichier
        
        Returns:
            dict: {
                "valid_order": bool,
                "error_code": "000000003" si ordre invalide,
                "message": message d'erreur
            }
        """
        
        try:
            # Extraire date du fichier actuel
            current_file_date = self.extract_date_from_filename(filename)
            
            if not current_file_date:
                # Si pas de date dans filename, on laisse passer
                return {
                    "valid_order": True,
                    "error_code": None,
                    "message": None
                }
            
            # Récupérer dernière date traitée pour cette table
            tracking_df = self.spark.table(self.tracking_table)
            
            last_date_df = tracking_df.filter(
                (col("table_name") == table_name) &
                (col("status") == "SUCCESS") &
                (col("file_date").isNotNull())
            ).select(spark_max("file_date").alias("last_date"))
            
            last_date_row = last_date_df.collect()
            
            if last_date_row and last_date_row[0]["last_date"]:
                last_date = last_date_row[0]["last_date"]
                
                # Vérifier ordre
                if current_file_date < last_date:
                    return {
                        "valid_order": False,
                        "error_code": "000000003",
                        "message": f"File date {current_file_date} < last processed date {last_date} for table {table_name}"
                    }
            
            return {
                "valid_order": True,
                "error_code": None,
                "message": None
            }
        
        except Exception as e:
            print(f"⚠️  Erreur vérification ordre fichier : {e}")
            # En cas d'erreur, on laisse passer
            return {
                "valid_order": True,
                "error_code": None,
                "message": None
            }
    
    def validate_file(self, table_name: str, filename: str) -> dict:
        """
        Validation complète du fichier (duplicate + ordre)
        
        Args:
            table_name: Nom de la table
            filename: Nom du fichier
        
        Returns:
            dict: {
                "valid": bool,
                "error_code": code erreur si invalide,
                "errors": liste des erreurs
            }
        """
        
        errors = []
        
        # Check 1: Duplicate
        duplicate_check = self.check_duplicate(table_name, filename)
        if duplicate_check["is_duplicate"]:
            errors.append({
                "type": "DUPLICATE_FILE",
                "error_code": duplicate_check["error_code"],
                "message": duplicate_check["message"]
            })
        
        # Check 2: File order
        order_check = self.check_file_order(table_name, filename)
        if not order_check["valid_order"]:
            errors.append({
                "type": "INVALID_FILE_ORDER",
                "error_code": order_check["error_code"],
                "message": order_check["message"]
            })
        
        return {
            "valid": len(errors) == 0,
            "error_code": errors[0]["error_code"] if errors else None,
            "errors": errors
        }
    
    def register_file(self, table_name: str, filename: str, status: str, row_count: int = None):
        """
        Enregistre un fichier traité dans le tracking
        
        Args:
            table_name: Nom de la table
            filename: Nom du fichier
            status: SUCCESS ou FAILED
            row_count: Nombre de lignes (optionnel)
        """
        
        try:
            # Extraire date du fichier
            file_date = self.extract_date_from_filename(filename)
            
            # Créer enregistrement
            tracking_record = self.spark.createDataFrame([{
                "table_name": table_name,
                "filename": filename,
                "file_date": file_date,
                "processed_date": datetime.now(),
                "status": status,
                "row_count": str(row_count) if row_count else None
            }])
            
            # Ajouter à la table
            tracking_record.write.format("delta").mode("append").saveAsTable(self.tracking_table)
            
            print(f"   📝 Fichier enregistré : {filename} ({status})")
        
        except Exception as e:
            print(f"⚠️  Erreur enregistrement tracking : {e}")
    
    def get_processed_files(self, table_name: str = None) -> list:
        """
        Récupère la liste des fichiers traités
        
        Args:
            table_name: Filtre par table (optionnel)
        
        Returns:
            Liste des fichiers traités
        """
        
        try:
            tracking_df = self.spark.table(self.tracking_table)
            
            if table_name:
                tracking_df = tracking_df.filter(col("table_name") == table_name)
            
            return tracking_df.select("table_name", "filename", "file_date", "processed_date", "status").collect()
        
        except Exception as e:
            print(f"⚠️  Erreur récupération fichiers traités : {e}")
            return []
    
    def display_tracking_summary(self):
        """Affiche un résumé du tracking"""
        
        try:
            tracking_df = self.spark.table(self.tracking_table)
            
            print("\n" + "=" * 80)
            print("📊 RÉSUMÉ TRACKING FICHIERS")
            print("=" * 80)
            
            # Total par statut
            summary_df = tracking_df.groupBy("status").count().orderBy("status")
            print("\n📈 Par statut :")
            summary_df.show(truncate=False)
            
            # Par table
            table_summary = tracking_df.groupBy("table_name").count().orderBy(col("count").desc())
            print("\n📋 Par table :")
            table_summary.show(10, truncate=False)
            
            # Derniers fichiers
            print("\n🕐 10 derniers fichiers :")
            last_files = tracking_df.orderBy(col("processed_date").desc()).limit(10)
            last_files.select("table_name", "filename", "processed_date", "status").show(truncate=False)
        
        except Exception as e:
            print(f"⚠️  Erreur affichage tracking : {e}")


def main():
    """Test du tracking manager"""
    
    import sys
    import os
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    
    from config import Config
    
    # Initialiser Spark
    spark = SparkSession.builder.appName("WAX-TrackingTest").getOrCreate()
    
    # Configuration
    config = Config(
        catalog="abu_catalog",
        schema_files="databricksassetbundletest",
        volume="externalvolumetes",
        schema_tables="gdp_poc_dev",
        env="dev"
    )
    
    # Créer tracking manager
    tracking = TrackingManager(spark, config)
    
    # Test 1: Valider nouveau fichier
    print("\n🧪 Test 1: Validation nouveau fichier")
    result = tracking.validate_file("customers", "customers_20251016_101112.csv")
    print(f"   Résultat: {result}")
    
    # Test 2: Enregistrer fichier
    print("\n🧪 Test 2: Enregistrement fichier")
    tracking.register_file("customers", "customers_20251016_101112.csv", "SUCCESS", 1000)
    
    # Test 3: Tester duplicate
    print("\n🧪 Test 3: Test duplicate")
    result = tracking.validate_file("customers", "customers_20251016_101112.csv")
    print(f"   Résultat: {result}")
    
    # Test 4: Tester ordre fichier
    print("\n🧪 Test 4: Test ordre fichier")
    result = tracking.validate_file("customers", "customers_20251015_101112.csv")
    print(f"   Résultat: {result}")
    
    # Afficher résumé
    tracking.display_tracking_summary()


if __name__ == "__main__":
    main()

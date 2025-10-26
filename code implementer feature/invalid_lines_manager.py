"""
invalid_lines_manager.py
Gestionnaire des lignes invalides
Sauvegarde les lignes rejetées dans une table dédiée
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType


class InvalidLinesManager:
    """
    Gestionnaire pour les lignes invalides
    
    Features:
    - Sauvegarde lignes rejetées dans table <table_name>_invalid_lines
    - Tracking raison du rejet
    - Métadonnées complètes
    """
    
    def __init__(self, spark: SparkSession, config):
        self.spark = spark
        self.config = config
        self.catalog = config.catalog
        self.schema = config.schema_tables
    
    def get_invalid_table_name(self, table_name: str) -> str:
        """Retourne le nom de la table invalid lines"""
        return f"{self.catalog}.{self.schema}.{table_name}_invalid_lines"
    
    def create_invalid_lines_table(self, table_name: str, source_schema: StructType = None):
        """
        Crée la table pour stocker les lignes invalides
        
        Args:
            table_name: Nom de la table source
            source_schema: Schéma de la table source (optionnel)
        """
        
        invalid_table = self.get_invalid_table_name(table_name)
        
        try:
            # Vérifier si existe déjà
            table_exists = self.spark.catalog.tableExists(invalid_table)
            
            if table_exists:
                print(f"   ℹ️  Table invalid lines existe : {invalid_table}")
                return
            
            print(f"   📊 Création table invalid lines : {invalid_table}")
            
            # Créer schéma pour invalid lines
            # On conserve toutes les colonnes source + métadonnées
            invalid_schema = StructType([
                StructField("invalid_line_id", LongType(), False),
                StructField("filename", StringType(), False),
                StructField("rejection_date", TimestampType(), False),
                StructField("rejection_reason", StringType(), False),
                StructField("error_type", StringType(), False),
                StructField("error_details", StringType(), True),
                StructField("line_number", LongType(), True),
                StructField("raw_data", StringType(), True)  # Données originales en string
            ])
            
            # Si on a le schéma source, on l'ajoute
            if source_schema:
                invalid_schema.fields.extend(source_schema.fields)
            
            # Créer table vide
            empty_df = self.spark.createDataFrame([], invalid_schema)
            empty_df.write.format("delta").saveAsTable(invalid_table)
            
            print(f"   ✅ Table créée")
        
        except Exception as e:
            print(f"   ⚠️  Erreur création table invalid lines : {e}")
    
    def save_invalid_lines(
        self, 
        df: DataFrame, 
        table_name: str,
        filename: str,
        rejection_reason: str,
        error_type: str = "VALIDATION_ERROR",
        error_details: str = None
    ):
        """
        Sauvegarde les lignes invalides dans la table dédiée
        
        Args:
            df: DataFrame contenant les lignes invalides
            table_name: Nom de la table source
            filename: Nom du fichier source
            rejection_reason: Raison du rejet
            error_type: Type d'erreur (VALIDATION_ERROR, TYPE_ERROR, etc.)
            error_details: Détails additionnels
        """
        
        if df.count() == 0:
            print(f"   ℹ️  Aucune ligne invalide à sauvegarder")
            return
        
        invalid_table = self.get_invalid_table_name(table_name)
        
        try:
            # Créer table si n'existe pas
            self.create_invalid_lines_table(table_name, df.schema)
            
            # Ajouter métadonnées
            invalid_df = df.withColumn("filename", lit(filename)) \
                           .withColumn("rejection_date", current_timestamp()) \
                           .withColumn("rejection_reason", lit(rejection_reason)) \
                           .withColumn("error_type", lit(error_type)) \
                           .withColumn("error_details", lit(error_details))
            
            # Ajouter ID et line_number si pas présents
            if "invalid_line_id" not in invalid_df.columns:
                from pyspark.sql.functions import monotonically_increasing_id
                invalid_df = invalid_df.withColumn("invalid_line_id", monotonically_increasing_id())
            
            if "line_number" not in invalid_df.columns:
                invalid_df = invalid_df.withColumn("line_number", lit(None).cast(LongType()))
            
            if "raw_data" not in invalid_df.columns:
                invalid_df = invalid_df.withColumn("raw_data", lit(None).cast(StringType()))
            
            # Sauvegarder
            invalid_df.write.format("delta").mode("append").saveAsTable(invalid_table)
            
            count = df.count()
            print(f"   📝 {count} ligne(s) invalide(s) sauvegardée(s) dans {invalid_table}")
        
        except Exception as e:
            print(f"   ⚠️  Erreur sauvegarde lignes invalides : {e}")
            import traceback
            traceback.print_exc()
    
    def get_invalid_lines_count(self, table_name: str) -> int:
        """
        Retourne le nombre de lignes invalides pour une table
        
        Args:
            table_name: Nom de la table
        
        Returns:
            Nombre de lignes invalides
        """
        
        invalid_table = self.get_invalid_table_name(table_name)
        
        try:
            if not self.spark.catalog.tableExists(invalid_table):
                return 0
            
            return self.spark.table(invalid_table).count()
        
        except Exception as e:
            print(f"⚠️  Erreur comptage lignes invalides : {e}")
            return 0
    
    def get_invalid_lines_summary(self, table_name: str = None):
        """
        Affiche un résumé des lignes invalides
        
        Args:
            table_name: Filtre par table (optionnel)
        """
        
        try:
            if table_name:
                invalid_table = self.get_invalid_table_name(table_name)
                
                if not self.spark.catalog.tableExists(invalid_table):
                    print(f"⚠️  Table {invalid_table} n'existe pas")
                    return
                
                df = self.spark.table(invalid_table)
                
                print(f"\n📊 Résumé lignes invalides : {table_name}")
                print("=" * 80)
                
                # Total
                total = df.count()
                print(f"Total lignes invalides : {total}")
                
                # Par type d'erreur
                print("\n📋 Par type d'erreur :")
                df.groupBy("error_type").count().orderBy(col("count").desc()).show(truncate=False)
                
                # Par raison de rejet
                print("\n📋 Par raison de rejet :")
                df.groupBy("rejection_reason").count().orderBy(col("count").desc()).show(truncate=False)
                
                # Dernières erreurs
                print("\n🕐 10 dernières erreurs :")
                df.orderBy(col("rejection_date").desc()).limit(10) \
                  .select("filename", "rejection_reason", "error_type", "rejection_date") \
                  .show(truncate=False)
            
            else:
                # Résumé global de toutes les tables
                print("\n📊 RÉSUMÉ GLOBAL LIGNES INVALIDES")
                print("=" * 80)
                
                # Lister toutes les tables invalid_lines
                all_tables = self.spark.catalog.listTables(self.schema)
                invalid_tables = [t.name for t in all_tables if t.name.endswith("_invalid_lines")]
                
                if not invalid_tables:
                    print("✅ Aucune table invalid_lines trouvée")
                    return
                
                print(f"📋 {len(invalid_tables)} table(s) invalid_lines trouvée(s)\n")
                
                for table in invalid_tables:
                    full_table = f"{self.catalog}.{self.schema}.{table}"
                    count = self.spark.table(full_table).count()
                    print(f"   • {table}: {count} ligne(s)")
        
        except Exception as e:
            print(f"⚠️  Erreur résumé lignes invalides : {e}")
    
    def cleanup_invalid_lines(self, table_name: str, days_to_keep: int = 30):
        """
        Nettoie les anciennes lignes invalides
        
        Args:
            table_name: Nom de la table
            days_to_keep: Nombre de jours à conserver
        """
        
        invalid_table = self.get_invalid_table_name(table_name)
        
        try:
            if not self.spark.catalog.tableExists(invalid_table):
                return
            
            from pyspark.sql.functions import date_sub, current_date
            
            # Supprimer lignes > days_to_keep
            cutoff_date = date_sub(current_date(), days_to_keep)
            
            df = self.spark.table(invalid_table)
            before_count = df.count()
            
            # Filtrer et réécrire
            df_cleaned = df.filter(col("rejection_date") >= cutoff_date)
            df_cleaned.write.format("delta").mode("overwrite").saveAsTable(invalid_table)
            
            after_count = df_cleaned.count()
            deleted = before_count - after_count
            
            print(f"   🧹 {deleted} ligne(s) invalide(s) supprimée(s) (> {days_to_keep} jours)")
        
        except Exception as e:
            print(f"⚠️  Erreur nettoyage lignes invalides : {e}")


def main():
    """Test invalid lines manager"""
    
    import sys
    import os
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    
    from config import Config
    from pyspark.sql.types import IntegerType
    
    print("🚀 Test Invalid Lines Manager")
    
    # Initialiser Spark
    spark = SparkSession.builder.appName("WAX-InvalidLinesTest").getOrCreate()
    
    # Configuration
    config = Config(
        catalog="abu_catalog",
        schema_files="databricksassetbundletest",
        volume="externalvolumetes",
        schema_tables="gdp_poc_dev",
        env="dev"
    )
    
    # Créer manager
    manager = InvalidLinesManager(spark, config)
    
    # Test 1: Créer table invalid lines
    print("\n🧪 Test 1: Création table invalid lines")
    schema = StructType([
        StructField("customer_id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True)
    ])
    manager.create_invalid_lines_table("customers", schema)
    
    # Test 2: Sauvegarder lignes invalides
    print("\n🧪 Test 2: Sauvegarde lignes invalides")
    test_data = [
        (None, "John Doe", "invalid-email"),
        (123, None, "test@example.com")
    ]
    test_df = spark.createDataFrame(test_data, schema)
    
    manager.save_invalid_lines(
        df=test_df,
        table_name="customers",
        filename="customers_20251016.csv",
        rejection_reason="Validation failed",
        error_type="NULL_VALUE",
        error_details="customer_id or name is NULL"
    )
    
    # Test 3: Afficher résumé
    print("\n🧪 Test 3: Résumé lignes invalides")
    manager.get_invalid_lines_summary("customers")
    
    # Test 4: Comptage
    print("\n🧪 Test 4: Comptage")
    count = manager.get_invalid_lines_count("customers")
    print(f"Total lignes invalides : {count}")


if __name__ == "__main__":
    main()

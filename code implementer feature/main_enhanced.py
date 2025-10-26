"""
main_enhanced.py
Module principal amélioré - WAX Data Ingestion Pipeline v2.0.0

Nouvelles features R1:
✅ Non-Zipped File Handling
✅ Duplicate Prevention (error 000000002)
✅ File Order Validation (error 000000003)
✅ Last vs All Tables (selon mode ingestion)
✅ Fail Fast Option (paramètre configurable)
✅ Invalid Lines Table
"""

import sys
import os
from datetime import datetime

# Imports des modules WAX
from config import Config
from file_handler import FileHandler
from tracking_manager import TrackingManager
from unzip_module import UnzipModule
from autoloader_module import AutoLoaderModule
from invalid_lines_manager import InvalidLinesManager
from ingestion_enhanced import IngestionManagerEnhanced
from delta_manager import DeltaManager
from logger_manager import LoggerManager
from validator import Validator
from column_processor import ColumnProcessor


class WAXPipelineEnhanced:
    """
    Pipeline d'ingestion WAX - Version améliorée
    
    Workflow:
    1. Traitement fichiers non-ZIP (nouveauté)
    2. Extraction ZIP
    3. Validation fichiers (duplicate + ordre) (nouveauté)
    4. Auto Loader
    5. Validation données
    6. Traitement colonnes
    7. Ingestion (Last + All selon mode) (amélioré)
    8. Logging
    """
    
    def __init__(self, spark, config: Config, config_api_params: dict = None):
        """
        Initialise le pipeline
        
        Args:
            spark: SparkSession
            config: Configuration
            config_api_params: Paramètres additionnels depuis API
        """
        
        self.spark = spark
        self.config = config
        self.api_params = config_api_params or {}
        
        # Initialiser managers
        self._init_managers()
        
        # Statistiques
        self.stats = {
            "start_time": datetime.now(),
            "files_processed": 0,
            "files_failed": 0,
            "files_skipped": 0,
            "rows_ingested": 0,
            "rows_rejected": 0,
            "errors": []
        }
    
    def _init_managers(self):
        """Initialise tous les managers"""
        
        print("\n" + "=" * 80)
        print("🚀 INITIALISATION WAX PIPELINE v2.0.0")
        print("=" * 80)
        
        # Managers de base
        self.delta_manager = DeltaManager(self.spark, self.config)
        self.logger_manager = LoggerManager(self.spark, self.config)
        self.validator = Validator(self.spark, self.config)
        self.column_processor = ColumnProcessor(self.spark, self.config)
        
        # Nouveaux managers
        self.file_handler = FileHandler(self.spark, self.config)
        self.tracking_manager = TrackingManager(self.spark, self.config)
        self.invalid_lines_manager = InvalidLinesManager(self.spark, self.config)
        
        # Modules ingestion
        self.unzip_module = UnzipModule(self.spark, self.config)
        self.autoloader = AutoLoaderModule(self.spark, self.config)
        self.ingestion_manager = IngestionManagerEnhanced(
            self.spark, self.config, self.delta_manager
        )
        
        print("✅ Tous les managers initialisés")
    
    def get_param(self, key: str, default=None):
        """Récupère paramètre depuis API ou default"""
        return self.api_params.get(key, default)
    
    def run(self, table_config_excel: str):
        """
        Exécute le pipeline complet
        
        Args:
            table_config_excel: Chemin vers Excel de configuration
        
        Returns:
            dict: Statistiques d'exécution
        """
        
        print("\n" + "=" * 80)
        print("🚀 DÉMARRAGE PIPELINE WAX v2.0.0")
        print("=" * 80)
        print(f"📅 Date: {datetime.now()}")
        print(f"📋 Config: {table_config_excel}")
        print(f"🌍 Env: {self.config.env}")
        
        try:
            # Étape 1: Traitement fichiers non-ZIP (NOUVEAU)
            if self.get_param("non_zip_support_enabled", True):
                print("\n" + "=" * 80)
                print("📄 ÉTAPE 1: TRAITEMENT FICHIERS NON-ZIP")
                print("=" * 80)
                non_zip_result = self.file_handler.process_non_zip_files()
                print(f"✅ Fichiers non-ZIP traités: {non_zip_result['file_count']}")
            
            # Étape 2: Extraction ZIP
            print("\n" + "=" * 80)
            print("📦 ÉTAPE 2: EXTRACTION ZIP")
            print("=" * 80)
            
            delete_zip = self.get_param("delete_zip_after_extract", True)
            unzip_result = self.unzip_module.process_zips(delete_after_extract=delete_zip)
            print(f"✅ Fichiers ZIP traités: {unzip_result['file_count']}")
            
            # Étape 3: Validation et Ingestion par table
            print("\n" + "=" * 80)
            print("🔍 ÉTAPE 3: VALIDATION ET INGESTION")
            print("=" * 80)
            
            self._process_tables(table_config_excel)
            
            # Étape 4: Logs finaux
            print("\n" + "=" * 80)
            print("📊 ÉTAPE 4: LOGS ET MÉTRIQUES")
            print("=" * 80)
            
            self.logger_manager.log_execution(
                table_name="pipeline_summary",
                status="SUCCESS",
                files_processed=self.stats["files_processed"],
                rows_processed=self.stats["rows_ingested"]
            )
            
            # Stats finales
            self._display_final_stats()
            
            return self.stats
        
        except Exception as e:
            print(f"\n❌ ERREUR PIPELINE: {e}")
            import traceback
            traceback.print_exc()
            
            self.stats["errors"].append({
                "type": "PIPELINE_ERROR",
                "message": str(e)
            })
            
            return self.stats
    
    def _process_tables(self, config_excel: str):
        """
        Traite toutes les tables depuis la config Excel
        
        Args:
            config_excel: Chemin Excel
        """
        
        # Lire configuration
        import pandas as pd
        
        try:
            file_table_df = pd.read_excel(config_excel, sheet_name="File-Table")
            field_column_df = pd.read_excel(config_excel, sheet_name="Field-Column")
        except Exception as e:
            print(f"❌ Erreur lecture config Excel: {e}")
            raise
        
        # Traiter chaque table
        for idx, row in file_table_df.iterrows():
            table_name = row.get("Delta Table Name")
            
            if not table_name:
                continue
            
            print(f"\n{'='*80}")
            print(f"📊 TRAITEMENT TABLE: {table_name}")
            print(f"{'='*80}")
            
            try:
                self._process_single_table(table_name, row, field_column_df)
                self.stats["files_processed"] += 1
            
            except Exception as e:
                print(f"❌ Erreur table {table_name}: {e}")
                self.stats["files_failed"] += 1
                self.stats["errors"].append({
                    "table": table_name,
                    "error": str(e)
                })
    
    def _process_single_table(self, table_name: str, table_config: dict, field_config):
        """
        Traite une seule table
        
        Args:
            table_name: Nom de la table
            table_config: Config de la table
            field_config: Config des colonnes
        """
        
        # Extraire config
        filename_pattern = table_config.get("Filename Pattern", "")
        ingestion_mode = table_config.get("Ingestion mode", "DELTA_FROM_FLOW")
        input_format = table_config.get("Input format", "csv")
        input_delimiter = table_config.get("Input delimiter", ",")
        ict_threshold = table_config.get("Invalid column per line tolerance", 
                                        self.get_param("ict_default", 10))
        rlt_threshold = table_config.get("Rejected line per file tolerance", 
                                        self.get_param("rlt_default", 10))
        
        # Colonnes pour cette table
        table_columns = field_config[field_config["Delta Table Name"] == table_name]
        
        # Lister fichiers extraits pour cette table
        extract_dir = f"{self.config.volume_base}/extracted/{table_name}"
        
        try:
            import os
            extract_dir_fs = extract_dir.replace("/Volumes", "/dbfs/Volumes")
            
            if not os.path.exists(extract_dir_fs):
                print(f"⚠️  Pas de fichiers pour {table_name}")
                return
            
            files = [f for f in os.listdir(extract_dir_fs) if f.endswith(('.csv', '.parquet'))]
            
            if not files:
                print(f"⚠️  Pas de fichiers à traiter pour {table_name}")
                return
            
            print(f"📄 {len(files)} fichier(s) trouvé(s)")
            
        except Exception as e:
            print(f"⚠️  Erreur listage fichiers: {e}")
            return
        
        # Traiter chaque fichier
        for filename in files:
            self._process_single_file(
                table_name=table_name,
                filename=filename,
                table_config=table_config,
                table_columns=table_columns,
                ingestion_mode=ingestion_mode
            )
    
    def _process_single_file(
        self, 
        table_name: str, 
        filename: str,
        table_config: dict,
        table_columns,
        ingestion_mode: str
    ):
        """
        Traite un seul fichier
        
        Args:
            table_name: Nom de la table
            filename: Nom du fichier
            table_config: Config table
            table_columns: Config colonnes
            ingestion_mode: Mode d'ingestion
        """
        
        print(f"\n{'─'*80}")
        print(f"📄 Fichier: {filename}")
        print(f"{'─'*80}")
        
        # NOUVEAU: Validation tracking (duplicate + ordre)
        if self.get_param("duplicate_check_enabled", True) or \
           self.get_param("file_order_check_enabled", True):
            
            validation = self.tracking_manager.validate_file(table_name, filename)
            
            if not validation["valid"]:
                print(f"❌ Fichier rejeté: {validation['errors']}")
                
                for error in validation["errors"]:
                    print(f"   • {error['type']}: {error['message']}")
                    print(f"   • Error code: {error['error_code']}")
                
                self.stats["files_skipped"] += 1
                self.stats["errors"].append({
                    "table": table_name,
                    "file": filename,
                    "errors": validation["errors"]
                })
                
                # Logger l'erreur
                self.tracking_manager.register_file(
                    table_name, filename, "FAILED", row_count=0
                )
                
                return
        
        # Auto Loader
        file_path = f"{self.config.volume_base}/extracted/{table_name}/{filename}"
        
        try:
            df = self.autoloader.load_file(
                file_path=file_path,
                file_format=table_config.get("Input format", "csv"),
                delimiter=table_config.get("Input delimiter", ",")
            )
            
            if df.count() == 0:
                print(f"⚠️  Fichier vide")
                self.stats["files_skipped"] += 1
                return
            
            print(f"✅ {df.count()} lignes lues")
            
        except Exception as e:
            print(f"❌ Erreur lecture: {e}")
            self.stats["files_failed"] += 1
            return
        
        # Validation données
        validation_result = self.validator.validate_data(
            df=df,
            column_defs=table_columns,
            filename=filename,
            ict_threshold=table_config.get("Invalid column per line tolerance", 10),
            rlt_threshold=table_config.get("Rejected line per file tolerance", 10)
        )
        
        if not validation_result["valid"]:
            print(f"❌ Validation échouée: {validation_result['errors']}")
            self.stats["files_failed"] += 1
            return
        
        # Traitement colonnes
        df_processed = self.column_processor.process_columns(
            df=df,
            column_defs=table_columns
        )
        
        # NOUVEAU: Sauvegarder lignes invalides si configuré
        if self.get_param("invalid_lines_generated", True):
            invalid_df = df_processed.filter("_validation_failed = true")
            
            if invalid_df.count() > 0:
                self.invalid_lines_manager.save_invalid_lines(
                    df=invalid_df,
                    table_name=table_name,
                    filename=filename,
                    rejection_reason="Validation failed",
                    error_type="DATA_VALIDATION"
                )
                
                # Filtrer les lignes valides
                df_processed = df_processed.filter("_validation_failed = false")
        
        # Ingestion (AMÉLIORÉ: gestion Last/All)
        stats = self.ingestion_manager.apply_ingestion_mode(
            df_raw=df_processed,
            column_defs=table_columns,
            table_name=table_name,
            ingestion_mode=ingestion_mode,
            file_name_received=filename,
            last_table_name=table_config.get("Last Table Name")  # Support nom personnalisé
        )
        
        # Mise à jour stats
        self.stats["rows_ingested"] += stats["rows_ingested"]
        
        # NOUVEAU: Enregistrer dans tracking
        self.tracking_manager.register_file(
            table_name=table_name,
            filename=filename,
            status="SUCCESS",
            row_count=stats["rows_ingested"]
        )
        
        print(f"✅ Fichier traité avec succès")
    
    def _display_final_stats(self):
        """Affiche les statistiques finales"""
        
        print("\n" + "=" * 80)
        print("📊 STATISTIQUES FINALES")
        print("=" * 80)
        
        print(f"\n⏱️  Durée totale: {datetime.now() - self.stats['start_time']}")
        print(f"\n📈 Fichiers:")
        print(f"   ✅ Traités: {self.stats['files_processed']}")
        print(f"   ❌ Échecs : {self.stats['files_failed']}")
        print(f"   ⏭️  Ignorés: {self.stats['files_skipped']}")
        
        print(f"\n📊 Données:")
        print(f"   ✅ Lignes ingérées: {self.stats['rows_ingested']:,}")
        print(f"   ❌ Lignes rejetées: {self.stats['rows_rejected']:,}")
        
        if self.stats["errors"]:
            print(f"\n⚠️  Erreurs ({len(self.stats['errors'])}):")
            for error in self.stats["errors"][:5]:  # Max 5
                print(f"   • {error}")
        
        print("\n" + "=" * 80)
        print("✅ PIPELINE TERMINÉ")
        print("=" * 80)


def main():
    """Point d'entrée principal"""
    
    from pyspark.sql import SparkSession
    
    print("""
    ╔══════════════════════════════════════════════════════════╗
    ║  WAX DATA INGESTION PIPELINE v2.0.0                      ║
    ║  Features: Non-ZIP, Tracking, Last/All, Fail Fast        ║
    ╚══════════════════════════════════════════════════════════╝
    """)
    
    # Spark
    spark = SparkSession.builder \
        .appName("WAX-Pipeline-Enhanced-v2") \
        .getOrCreate()
    
    # Configuration
    config = Config(
        catalog="abu_catalog",
        schema_files="databricksassetbundletest",
        volume="externalvolumetes",
        schema_tables="gdp_poc_dev",
        env="dev"
    )
    
    # Paramètres API (exemple)
    api_params = {
        "fail_fast_enabled": True,
        "fail_fast_threshold": 10,
        "invalid_lines_generated": True,
        "duplicate_check_enabled": True,
        "file_order_check_enabled": True,
        "non_zip_support_enabled": True,
        "delete_zip_after_extract": True
    }
    
    # Pipeline
    pipeline = WAXPipelineEnhanced(spark, config, api_params)
    
    # Configuration Excel
    config_excel = "/dbfs/mnt/config/wax_config.xlsx"  # À adapter
    
    # Exécuter
    stats = pipeline.run(config_excel)
    
    print(f"\n✅ Pipeline terminé avec succès")
    print(f"📊 Stats: {stats}")


if __name__ == "__main__":
    main()

"""
ingestion_enhanced.py
Modes d'ingestion améliorés - Conformes aux spécifications Gherkin
Gestion correcte des tables Last vs All selon le mode
"""

from datetime import datetime
from pyspark.sql import functions as F
from delta.tables import DeltaTable


class IngestionManagerEnhanced:
    """
    Gestionnaire modes d'ingestion - Version améliorée
    
    Modes conformes aux specs:
    - FULL_SNAPSHOT : Crée/met à jour LAST et ALL
    - DELTA_FROM_FLOW : Met à jour ALL uniquement (pas de LAST)
    - DELTA_FROM_NON_HISTORIZED : Merge sur clés (LAST + ALL)
    - DELTA_FROM_HISTORIZED : Append historique (LAST + ALL)
    - FULL_KEY_REPLACE : Delete puis insert sur clés (LAST + ALL)
    """

    def __init__(self, spark, config, delta_manager):
        self.spark = spark
        self.config = config
        self.delta_manager = delta_manager

    def get_table_names(self, base_table_name: str, last_table_name_override: str = None) -> dict:
        """
        Génère les noms des tables Last et All
        
        Args:
            base_table_name: Nom de base
            last_table_name_override: Nom personnalisé pour Last (optionnel)
        
        Returns:
            dict: {"all": "...", "last": "..."}
        """
        
        # Table All: toujours <base>_all
        table_all = self.config.get_table_full_name(f"{base_table_name}_all")
        
        # Table Last: personnalisé ou <base>_last
        if last_table_name_override:
            table_last = self.config.get_table_full_name(last_table_name_override)
        else:
            table_last = self.config.get_table_full_name(f"{base_table_name}_last")
        
        return {
            "all": table_all,
            "last": table_last
        }

    def apply_ingestion_mode(
        self, 
        df_raw, 
        column_defs, 
        table_name: str,
        ingestion_mode: str, 
        zone: str = "internal",
        parts: dict = None, 
        file_name_received: str = None,
        last_table_name: str = None
    ):
        """
        Applique le mode d'ingestion selon les spécifications
        
        Args:
            df_raw: DataFrame à ingérer
            column_defs: Définitions colonnes
            table_name: Nom de base de la table
            ingestion_mode: Mode (FULL_SNAPSHOT, DELTA_FROM_FLOW, etc.)
            zone: Zone (internal/external)
            parts: Partitions
            file_name_received: Nom fichier source
            last_table_name: Nom personnalisé pour table Last (optionnel)
        
        Returns:
            dict: Statistiques ingestion
        """
        
        # Générer noms tables
        tables = self.get_table_names(table_name, last_table_name)
        table_all = tables["all"]
        table_last = tables["last"]
        
        print(f"\n{'='*80}")
        print(f"🔄 INGESTION MODE: {ingestion_mode}")
        print(f"{'='*80}")
        print(f"📊 Table All  : {table_all}")
        print(f"📊 Table Last : {table_last}")
        
        # Extraire clés de merge
        specials = column_defs.copy()
        specials["Is Special lower"] = specials["Is Special"].astype(str).str.lower()
        merge_keys = specials[specials["Is Special lower"] == "ismergekey"]["Column Name"].tolist()
        update_cols = specials[specials["Is Special lower"] == "isstartvalidity"]["Column Name"].tolist()
        update_col = update_cols[0] if update_cols else None
        
        imode = (ingestion_mode or "").strip().upper()
        
        stats = {
            "mode": imode,
            "table_all": table_all,
            "table_last": table_last,
            "rows_ingested": df_raw.count(),
            "all_created": False,
            "last_created": False
        }
        
        # Dispatcher selon mode
        if imode == "FULL_SNAPSHOT":
            self._mode_full_snapshot(df_raw, table_all, table_last, parts, file_name_received, stats)
        
        elif imode == "DELTA_FROM_FLOW":
            self._mode_delta_from_flow(df_raw, table_all, table_last, parts, file_name_received, stats)
        
        elif imode == "DELTA_FROM_NON_HISTORIZED":
            self._mode_delta_non_historized(df_raw, table_all, table_last, merge_keys, 
                                           update_col, parts, file_name_received, stats)
        
        elif imode == "DELTA_FROM_HISTORIZED":
            self._mode_delta_historized(df_raw, table_all, table_last, parts, file_name_received, stats)
        
        elif imode == "FULL_KEY_REPLACE":
            self._mode_full_key_replace(df_raw, table_all, table_last, merge_keys, 
                                       parts, file_name_received, stats)
        else:
            # Mode par défaut: DELTA_FROM_FLOW
            print(f"⚠️  Mode inconnu '{imode}', utilisation DELTA_FROM_FLOW par défaut")
            self._mode_delta_from_flow(df_raw, table_all, table_last, parts, file_name_received, stats)
        
        print(f"{'='*80}")
        print(f"✅ Ingestion terminée")
        print(f"{'='*80}\n")
        
        return stats

    def _mode_full_snapshot(self, df_raw, table_all: str, table_last: str,
                           parts: dict, file_name_received: str, stats: dict):
        """
        FULL_SNAPSHOT : Crée/met à jour Last ET All
        
        Spec:
        Given the ingestion mode is "FULL_SNAPSHOT"
        When a new file is received
        Then create or update "<last_table>" containing the new file
        And create or update "<all_table>" containing all files received
        """
        
        print(f"\n📄 Mode: FULL_SNAPSHOT")
        print(f"   → Mise à jour Last (overwrite)")
        print(f"   → Mise à jour All (append)")
        
        # 1. Last: Overwrite (dernières données)
        self.delta_manager.save_delta(
            df_raw, table_last, mode="overwrite",
            parts=parts, file_name_received=file_name_received
        )
        stats["last_created"] = True
        
        # 2. All: Append (historique complet)
        self.delta_manager.save_delta(
            df_raw, table_all, mode="append", add_ts=True,
            parts=parts, file_name_received=file_name_received
        )
        stats["all_created"] = True

    def _mode_delta_from_flow(self, df_raw, table_all: str, table_last: str,
                             parts: dict, file_name_received: str, stats: dict):
        """
        DELTA_FROM_FLOW : Met à jour All uniquement (PAS de Last)
        
        Spec:
        Given the ingestion mode is "DELTA_FROM_FLOW"
        When a new file is received
        Then create or update "<all_table>" containing all files received
        (Note: NO Last table for this mode)
        """
        
        print(f"\n📄 Mode: DELTA_FROM_FLOW")
        print(f"   → Mise à jour All uniquement (append)")
        print(f"   → Last table NON créée (conforme specs)")
        
        # All uniquement: Append
        self.delta_manager.save_delta(
            df_raw, table_all, mode="append", add_ts=True,
            parts=parts, file_name_received=file_name_received
        )
        stats["all_created"] = True
        stats["last_created"] = False  # Important: pas de Last pour ce mode

    def _mode_delta_non_historized(self, df_raw, table_all: str, table_last: str,
                                   merge_keys: list, update_col: str, parts: dict,
                                   file_name_received: str, stats: dict):
        """
        DELTA_FROM_NON_HISTORIZED : Merge sur clés (Last + All)
        """
        
        if not merge_keys:
            raise ValueError(f"DELTA_FROM_NON_HISTORIZED requires merge keys")
        
        print(f"\n📄 Mode: DELTA_FROM_NON_HISTORIZED")
        print(f"   → Clés merge: {merge_keys}")
        print(f"   → Merge Last (update si plus récent)")
        print(f"   → Append All")
        
        fallback_col = "FILE_PROCESS_DATE"
        eff_update = update_col or fallback_col
        
        # 1. All: Append
        self.delta_manager.save_delta(
            df_raw, table_all, mode="append", add_ts=True,
            parts=parts, file_name_received=file_name_received
        )
        stats["all_created"] = True
        
        # 2. Last: Merge
        if not self.spark.catalog.tableExists(table_last):
            self.delta_manager.save_delta(
                df_raw, table_last, mode="overwrite",
                parts=parts, file_name_received=file_name_received
            )
        else:
            dt = DeltaTable.forName(self.spark, table_last)
            merge_cond = " AND ".join([f"target.`{k}` = source.`{k}`" for k in merge_keys])
            
            dt.alias("target").merge(
                df_raw.alias("source"),
                merge_cond
            ).whenMatchedUpdate(
                condition=f"source.`{eff_update}` > target.`{eff_update}`",
                set={c: f"source.`{c}`" for c in df_raw.columns}
            ).whenNotMatchedInsertAll().execute()
        
        stats["last_created"] = True

    def _mode_delta_historized(self, df_raw, table_all: str, table_last: str,
                              parts: dict, file_name_received: str, stats: dict):
        """
        DELTA_FROM_HISTORIZED : Append historique (Last + All)
        """
        
        print(f"\n📄 Mode: DELTA_FROM_HISTORIZED")
        print(f"   → Append Last")
        print(f"   → Append All")
        
        # 1. Last: Append
        self.delta_manager.save_delta(
            df_raw, table_last, mode="append", add_ts=True,
            parts=parts, file_name_received=file_name_received
        )
        stats["last_created"] = True
        
        # 2. All: Append
        self.delta_manager.save_delta(
            df_raw, table_all, mode="append", add_ts=True,
            parts=parts, file_name_received=file_name_received
        )
        stats["all_created"] = True

    def _mode_full_key_replace(self, df_raw, table_all: str, table_last: str,
                               merge_keys: list, parts: dict,
                               file_name_received: str, stats: dict):
        """
        FULL_KEY_REPLACE : Delete puis insert sur clés (Last + All)
        """
        
        if not merge_keys:
            raise ValueError(f"FULL_KEY_REPLACE requires merge keys")
        
        print(f"\n📄 Mode: FULL_KEY_REPLACE")
        print(f"   → Clés: {merge_keys}")
        print(f"   → Delete puis insert Last")
        print(f"   → Append All")
        
        # 1. All: Append
        self.delta_manager.save_delta(
            df_raw, table_all, mode="append", add_ts=True,
            parts=parts, file_name_received=file_name_received
        )
        stats["all_created"] = True
        
        # 2. Last: Delete + Insert
        if not self.spark.catalog.tableExists(table_last):
            self.delta_manager.save_delta(
                df_raw, table_last, mode="overwrite",
                parts=parts, file_name_received=file_name_received
            )
        else:
            # Delete existants
            key_vals = df_raw.select(merge_keys).distinct().collect()
            dt = DeltaTable.forName(self.spark, table_last)
            
            for row in key_vals:
                del_cond = " AND ".join([
                    f"`{k}` = {repr(row[k])}" for k in merge_keys
                ])
                dt.delete(del_cond)
            
            # Insert nouveaux
            self.delta_manager.save_delta(
                df_raw, table_last, mode="append", add_ts=True,
                parts=parts, file_name_received=file_name_received
            )
        
        stats["last_created"] = True


def main():
    """Test ingestion enhanced"""
    
    import sys
    import os
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    
    from pyspark.sql import SparkSession
    from config import Config
    from delta_manager import DeltaManager
    from pyspark.sql.types import StructType, StructField, IntegerType, StringType
    import pandas as pd
    
    print("🚀 Test Ingestion Enhanced")
    
    # Spark
    spark = SparkSession.builder.appName("WAX-IngestionTest").getOrCreate()
    
    # Config
    config = Config(
        catalog="abu_catalog",
        schema_files="databricksassetbundletest",
        volume="externalvolumetes",
        schema_tables="gdp_poc_dev",
        env="dev"
    )
    
    # Delta manager
    delta_manager = DeltaManager(spark, config)
    
    # Ingestion manager
    ingestion = IngestionManagerEnhanced(spark, config, delta_manager)
    
    # Données test
    schema = StructType([
        StructField("customer_id", IntegerType(), False),
        StructField("name", StringType(), True)
    ])
    
    test_data = [(1, "Alice"), (2, "Bob"), (3, "Charlie")]
    df_test = spark.createDataFrame(test_data, schema)
    
    # Column defs mock
    column_defs = pd.DataFrame({
        "Column Name": ["customer_id", "name"],
        "Is Special": ["IsMergeKey", ""]
    })
    
    # Test 1: FULL_SNAPSHOT
    print("\n" + "="*80)
    print("🧪 Test 1: FULL_SNAPSHOT (crée Last + All)")
    print("="*80)
    stats = ingestion.apply_ingestion_mode(
        df_test, column_defs, "test_customers", "FULL_SNAPSHOT",
        file_name_received="test.csv"
    )
    print(f"Stats: {stats}")
    
    # Test 2: DELTA_FROM_FLOW
    print("\n" + "="*80)
    print("🧪 Test 2: DELTA_FROM_FLOW (All uniquement)")
    print("="*80)
    stats = ingestion.apply_ingestion_mode(
        df_test, column_defs, "test_orders", "DELTA_FROM_FLOW",
        file_name_received="test.csv"
    )
    print(f"Stats: {stats}")
    print(f"✅ Last created: {stats['last_created']} (devrait être False)")


if __name__ == "__main__":
    main()

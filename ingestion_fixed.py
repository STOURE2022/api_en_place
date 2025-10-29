"""
ingestion.py - VERSION CORRIGÉE
Modes d'ingestion - Unity Catalog tables managées
✅ Support noms de tables customisés (last_table_name_conf, delta_table_name_conf)
"""

from datetime import datetime
from pyspark.sql import functions as F
from delta.tables import DeltaTable


class IngestionManager:
    """Gestionnaire modes d'ingestion - Unity Catalog"""

    def __init__(self, spark, config, delta_manager):
        self.spark = spark
        self.config = config
        self.delta_manager = delta_manager

    # ==================== ✅ NOUVEAU : GESTION NOMS DE TABLES ====================
    
    def _get_table_names(self, base_table_name: str, table_config: dict) -> tuple:
        """
        ✅ NOUVELLE FONCTION : Détermine les noms de tables selon configuration
        
        Règles :
        1. Si last_table_name_conf est défini → utiliser ce nom
        2. Sinon → utiliser delta_table_name_conf + suffixe "_last"
        3. Pour _all → toujours delta_table_name_conf + suffixe "_all"
        
        Args:
            base_table_name: Nom de la table de base
            table_config: Configuration de la table depuis Excel (row)
            
        Returns:
            (table_name_all, table_name_last)
        """
        
        # Récupérer configurations
        last_table_name_conf = str(table_config.get("last_table_name_conf", "")).strip()
        delta_table_name_conf = str(table_config.get("delta_table_name_conf", "")).strip()
        
        # Si pas de delta_table_name_conf, utiliser base_table_name
        if not delta_table_name_conf or delta_table_name_conf.lower() in ["", "nan", "none"]:
            delta_table_name_conf = base_table_name
        
        # ✅ Nom de la table _all (historique)
        table_name_all_simple = f"{delta_table_name_conf}_all"
        table_name_all = self.config.get_table_full_name(table_name_all_simple)
        
        # ✅ Nom de la table _last (état actuel)
        if last_table_name_conf and last_table_name_conf.lower() not in ["", "nan", "none"]:
            # Scénario 1 : Nom customisé défini
            table_name_last_simple = last_table_name_conf
            print(f"   📋 Utilisation nom customisé pour _last : {last_table_name_conf}")
        else:
            # Scénario 2 : Nom standard avec suffixe "_last"
            table_name_last_simple = f"{delta_table_name_conf}_last"
            print(f"   📋 Utilisation nom standard pour _last : {table_name_last_simple}")
        
        table_name_last = self.config.get_table_full_name(table_name_last_simple)
        
        return table_name_all, table_name_last

    def apply_ingestion_mode(self, df_raw, column_defs, table_name: str,
                             ingestion_mode: str, zone: str = "internal",
                             parts: dict = None, file_name_received: str = None,
                             table_config: dict = None):
        """
        Applique mode ingestion

        Modes:
        - FULL_SNAPSHOT : Écrase tout
        - DELTA_FROM_FLOW : Append simple
        - DELTA_FROM_NON_HISTORIZED : Merge sur clés
        - DELTA_FROM_HISTORIZED : Append historique
        - FULL_KEY_REPLACE : Delete puis insert sur clés
        
        Args:
            table_config: Configuration de la table (row depuis Excel)
        """

        # ✅ Utiliser nouvelle fonction pour déterminer noms de tables
        if table_config is None:
            # Fallback : mode ancien (backward compatibility)
            table_name_all = self.config.get_table_full_name(f"{table_name}_all")
            table_name_last = self.config.get_table_full_name(f"{table_name}_last")
            print("   ⚠️  table_config non fourni, utilisation noms par défaut")
        else:
            table_name_all, table_name_last = self._get_table_names(table_name, table_config)

        # Extraire clés de merge
        specials = column_defs.copy()
        specials["Is Special lower"] = specials["Is Special"].astype(str).str.lower()
        merge_keys = specials[specials["Is Special lower"] == "ismergekey"]["Column Name"].tolist()
        update_cols = specials[specials["Is Special lower"] == "isstartvalidity"]["Column Name"].tolist()
        update_col = update_cols[0] if update_cols else None

        imode = (ingestion_mode or "").strip().upper()
        print(f"🔄 Mode ingestion : {imode}")
        print(f"   📊 Table _all  : {table_name_all}")
        print(f"   📊 Table _last : {table_name_last}")

        # Toujours sauvegarder dans _all (historique)
        self.delta_manager.save_delta(
            df_raw, table_name_all, mode="append", add_ts=True,
            parts=parts, file_name_received=file_name_received
        )

        # Appliquer mode pour _last
        if imode == "FULL_SNAPSHOT":
            self._mode_full_snapshot(df_raw, table_name_last, parts, file_name_received)

        elif imode == "DELTA_FROM_FLOW":
            self._mode_delta_from_flow(df_raw, table_name_last, parts, file_name_received)

        elif imode == "DELTA_FROM_NON_HISTORIZED":
            self._mode_delta_non_historized(df_raw, table_name_last, merge_keys,
                                            update_col, parts, file_name_received)

        elif imode == "DELTA_FROM_HISTORIZED":
            self._mode_delta_historized(df_raw, table_name_last, parts, file_name_received)

        elif imode == "FULL_KEY_REPLACE":
            self._mode_full_key_replace(df_raw, table_name_last, merge_keys,
                                        parts, file_name_received)
        else:
            # Mode par défaut: append
            self.delta_manager.save_delta(
                df_raw, table_name_last, mode="append", add_ts=True,
                parts=parts, file_name_received=file_name_received
            )

    def _mode_full_snapshot(self, df_raw, table_name_last: str,
                            parts: dict, file_name_received: str):
        """FULL_SNAPSHOT: écrase tout"""
        self.delta_manager.save_delta(
            df_raw, table_name_last, mode="overwrite",
            parts=parts, file_name_received=file_name_received
        )

    def _mode_delta_from_flow(self, df_raw, table_name_last: str,
                              parts: dict, file_name_received: str):
        """DELTA_FROM_FLOW: append simple"""
        self.delta_manager.save_delta(
            df_raw, table_name_last, mode="append", add_ts=True,
            parts=parts, file_name_received=file_name_received
        )

    def _mode_delta_non_historized(self, df_raw, table_name_last: str,
                                   merge_keys: list, update_col: str, parts: dict,
                                   file_name_received: str):
        """DELTA_FROM_NON_HISTORIZED: merge avec update si plus récent"""
        if not merge_keys:
            raise ValueError(f"No merge keys for {table_name_last}")

        fallback_col = "FILE_PROCESS_DATE"
        compare_col = update_col if update_col else fallback_col
        auto_cols = ["FILE_PROCESS_DATE", "yyyy", "mm", "dd"]

        # Préparer colonne de comparaison
        if compare_col in df_raw.columns:
            compare_dtype = str(df_raw.schema[compare_col].dataType)
            if compare_dtype == "StringType":
                df_raw = df_raw.withColumn(compare_col, F.to_timestamp(compare_col))

        # Ajouter métadonnées
        df_raw = (
            df_raw
            .withColumn("FILE_PROCESS_DATE", F.current_timestamp())
            .withColumn("yyyy", F.lit(parts.get("yyyy", datetime.today().year)).cast("int"))
            .withColumn("mm", F.lit(parts.get("mm", datetime.today().month)).cast("int"))
            .withColumn("dd", F.lit(parts.get("dd", datetime.today().day)).cast("int"))
        )

        updates = df_raw.alias("updates")

        # Vérifier si table existe
        if not self.delta_manager.table_exists(table_name_last):
            # Première insertion
            self.delta_manager.save_delta(
                df_raw, table_name_last, mode="overwrite", add_ts=False,
                parts=parts, file_name_received=file_name_received
            )
            return

        # Récupérer table cible
        target = self.delta_manager.get_delta_table(table_name_last)
        target_cols = [f.name for f in target.toDF().schema.fields]

        update_cols_clean = [c for c in df_raw.columns
                             if c in target_cols and c not in merge_keys and c not in auto_cols]
        insert_cols_clean = [c for c in df_raw.columns
                             if c in target_cols and c not in auto_cols]

        update_expr = {c: f"updates.{c}" for c in update_cols_clean}
        insert_expr = {c: f"updates.{c}" for c in insert_cols_clean}

        cond = " AND ".join([f"target.{k}=updates.{k}" for k in merge_keys])

        # Merge
        try:
            (target.alias("target")
             .merge(updates, cond)
             .whenMatchedUpdate(condition=f"updates.{compare_col} > target.{compare_col}",
                              set=update_expr)
             .whenNotMatchedInsert(values=insert_expr)
             .execute())
            print(f"✅ Merge réussi sur {compare_col}")
        except Exception as e:
            print(f"❌ Erreur merge : {e}")
            raise

    def _mode_delta_historized(self, df_raw, table_name_last: str,
                               parts: dict, file_name_received: str):
        """DELTA_FROM_HISTORIZED: append avec historique"""
        self.delta_manager.save_delta(
            df_raw, table_name_last, mode="append", add_ts=True,
            parts=parts, file_name_received=file_name_received
        )

    def _mode_full_key_replace(self, df_raw, table_name_last: str,
                               merge_keys: list, parts: dict,
                               file_name_received: str):
        """FULL_KEY_REPLACE: delete puis insert sur clés"""
        if not merge_keys:
            raise ValueError(f"No merge keys for {table_name_last}")

        # Vérifier si table existe
        if not self.delta_manager.table_exists(table_name_last):
            # Première insertion
            self.delta_manager.save_delta(
                df_raw, table_name_last, mode="overwrite", add_ts=True,
                parts=parts, file_name_received=file_name_received
            )
            return

        try:
            target = self.delta_manager.get_delta_table(table_name_last)

            # Construire condition de suppression
            conditions = []
            for k in merge_keys:
                values = df_raw.select(k).distinct().rdd.flatMap(lambda x: x).collect()
                values_str = ','.join([f"'{str(x)}'" for x in values])
                conditions.append(f"{k} IN ({values_str})")
            cond = " OR ".join(conditions)

            # Supprimer puis insérer
            target.delete(condition=cond)
            self.delta_manager.save_delta(
                df_raw, table_name_last, mode="append", add_ts=True,
                parts=parts, file_name_received=file_name_received
            )
            print(f"✅ FULL_KEY_REPLACE réussi")
        except Exception as e:
            print(f"❌ Erreur FULL_KEY_REPLACE : {e}")
            raise

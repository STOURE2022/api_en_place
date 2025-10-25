"""
validator.py
Validation des données et qualité
"""

from datetime import datetime
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from .utils import parse_bool, TYPE_MAPPING


class DataValidator:
    """Validateur de données"""

    def __init__(self, spark):
        self.spark = spark
        self.ERROR_SCHEMA = self._build_error_schema()

    def _build_error_schema(self):
        """Schéma unifié pour erreurs"""
        return StructType([
            StructField("table_name", StringType(), True),
            StructField("filename", StringType(), True),
            StructField("column_name", StringType(), True),
            StructField("error_message", StringType(), True),
            StructField("raw_value", StringType(), True),
            StructField("error_count", IntegerType(), True)
        ])

    # ==================== VALIDATION FILENAME ====================

    def validate_filename(self, fname: str, source_table: str, matched_uri: str,
                          log_quality_path: str) -> bool:
        """Valide date dans nom fichier"""
        import os
        from .utils import extract_parts_from_filename

        base = os.path.basename(fname)
        print(f"🔍 Validation : {base}")

        error_schema = StructType([
            StructField("table_name", StringType(), True),
            StructField("filename", StringType(), True),
            StructField("column_name", StringType(), True),
            StructField("line_id", IntegerType(), True),
            StructField("invalid_value", StringType(), True),
            StructField("error_message", StringType(), True),
            StructField("uri", StringType(), True),
        ])

        parts = extract_parts_from_filename(base)
        if not parts:
            print(f"❌ Rejeté : {base}")
            err_data = [(source_table, base, "filename", None, None,
                         "Missing date pattern", matched_uri)]
            err_df = self.spark.createDataFrame(err_data, error_schema)

            try:
                err_df.write.format("delta").mode("append").save(log_quality_path)
            except:
                err_df.write.format("delta").mode("overwrite").save(log_quality_path)
            return False

        try:
            yyyy = parts.get("yyyy")
            mm = parts.get("mm")
            dd = parts.get("dd", 1)

            if not yyyy or not mm:
                raise ValueError("Missing year or month")
            if not (1900 <= yyyy <= 2100):
                raise ValueError(f"Year {yyyy} out of range")
            if not (1 <= mm <= 12):
                raise ValueError(f"Month {mm} invalid")
            if not (1 <= dd <= 31):
                raise ValueError(f"Day {dd} invalid")

            datetime(yyyy, mm, dd)
            print(f"✅ Accepté : {base} ({yyyy}-{mm:02d}-{dd:02d})")
            return True

        except (ValueError, TypeError) as e:
            print(f"❌ Rejeté : {base} ({e})")
            date_str = f"{yyyy}-{mm:02d}-{dd:02d}" if yyyy and mm else "N/A"
            err_data = [(source_table, base, "filename", None, date_str,
                         f"Invalid date: {e}", matched_uri)]
            err_df = self.spark.createDataFrame(err_data, error_schema)

            try:
                err_df.write.format("delta").mode("append").save(log_quality_path)
            except:
                err_df.write.format("delta").mode("overwrite").save(log_quality_path)
            return False

    # ==================== VALIDATION COLONNES ====================

    def validate_columns_presence(self, df: DataFrame, expected_cols: list,
                                table_name: str, filename: str) -> dict:
        """
        Vérifie présence des colonnes attendues
        Les colonnes sont déjà normalisées en lowercase par l'Auto Loader
        
        Args:
            df: DataFrame à valider (colonnes déjà en lowercase)
            expected_cols: Liste des colonnes attendues (en lowercase depuis Excel)
            table_name: Nom de la table
            filename: Nom du fichier
            
        Returns:
            dict: {
                "missing_columns": list,
                "df_errors": DataFrame ou None
            }
        """
        
        # Identifier colonnes manquantes
        # Les colonnes sont déjà normalisées, comparaison directe
        df_cols = set(df.columns)
        expected = set(expected_cols)
        
        # Colonnes vraiment manquantes (pas dans le CSV)
        missing_cols = expected - df_cols
        
        if not missing_cols:
            print(f"✅ Validation colonnes : Toutes les colonnes présentes")
            return {
                "missing_columns": [],
                "df_errors": None
            }
        
        # Il y a des colonnes manquantes (absentes du CSV source)
        missing_list = sorted(missing_cols)
        print(f"ℹ️  Colonnes absentes du CSV (seront ajoutées avec NULL) : {len(missing_list)}")
        print(f"   {missing_list[:5]}{'...' if len(missing_list) > 5 else ''}")
        
        # Créer DataFrame d'info (pas d'erreur, juste pour traçabilité)
        df_info = self.spark.createDataFrame(
            [(filename, col, "INFO_MISSING_COLUMN") for col in missing_list],
            "filename STRING, column_name STRING, error_type STRING"
        )
        
        df_info = (
            df_info
            .withColumn("table_name", F.lit(table_name))
            .withColumn("error_message", 
                    F.concat(F.lit("Column '"), F.col("column_name"), 
                            F.lit("' not present in source CSV, added with NULL")))
            .withColumn("raw_value", F.lit(None).cast("string"))
        )
        
        return {
            "missing_columns": missing_list,
            "df_errors": df_info
        }

    # ==================== VALIDATION FINALE TYPES ====================

    def validate_and_rebuild_dataframe(self, df: DataFrame, column_defs,
                                       table_name: str, filename: str) -> tuple:
        """
        Validation finale + reconstruction DataFrame (évite doublons colonnes)

        Returns:
            (df_validated, errors_list, column_errors_dict)
        """
        print(f"🔧 Validation types finale...")

        validated_columns = {}
        column_errors = {}
        errors_list = []

        # Copier colonnes non-métier
        metier_cols = [r["Column Name"] for _, r in column_defs.iterrows()]
        for c in df.columns:
            if c not in metier_cols:
                validated_columns[c] = df[c]

        # Valider colonnes métier
        for _, col_def in column_defs.iterrows():
            cname = str(col_def.get("Column Name")).strip()
            expected_type = str(col_def.get("Field type")).strip().upper()
            is_nullable = parse_bool(col_def.get("Is Nullable", "true"), True)

            if cname not in df.columns or expected_type not in TYPE_MAPPING:
                continue

            df = df.withColumn(f"{cname}_raw", F.col(cname))

            # Normaliser les chaînes vides -> null
            df = df.withColumn(
                cname,
                F.when(F.trim(F.col(cname)) == "", None).otherwise(F.col(cname))
            )

            # Appliquer try_cast
            col_test = F.expr(f"try_cast({cname} as {expected_type})")
            safe_cast = df.withColumn(f"{cname}_cast", col_test)

            # Erreurs de type
            invalid_rows = safe_cast.filter(
                F.col(f"{cname}_cast").isNull() & F.col(cname).isNotNull()
            )
            invalid_count = invalid_rows.count()

            if invalid_count > 0:
                if "line_id" not in invalid_rows.columns:
                    window = Window.orderBy(F.monotonically_increasing_id())
                    invalid_rows = invalid_rows.withColumn("line_id", F.row_number().over(window))

                err = invalid_rows.limit(1000).select(
                    F.lit(table_name).alias("table_name"),
                    F.lit(filename).alias("filename"),
                    F.lit(cname).alias("column_name"),
                    F.concat(
                        F.lit(f"TYPE MISMATCH: colonne '{cname}' (type attendu: {expected_type}), valeur: "),
                        F.col(f"{cname}_raw").cast("string"),
                        F.lit("")
                    ).alias("error_message"),
                    F.col(f"{cname}_raw").cast("string").alias("raw_value"),
                    F.lit(1).alias("error_count")
                )

                errors_list.append(err)
                column_errors.setdefault(cname, []).append(f"{invalid_count} erreurs de type")

            # Vérifier nullabilité
            if not is_nullable:
                null_rows = safe_cast.filter(F.col(f"{cname}_cast").isNull())
                null_count = null_rows.count()

                if null_count > 0:
                    if "line_id" not in null_rows.columns:
                        window = Window.orderBy(F.monotonically_increasing_id())
                        null_rows = null_rows.withColumn("line_id", F.row_number().over(window))

                    err = (
                        null_rows.limit(1000)
                        .select(
                            F.lit(table_name).alias("table_name"),
                            F.lit(filename).alias("filename"),
                            F.lit(cname).alias("column_name"),
                            F.concat(
                                F.lit(f"NULL VALUE: colonne '{cname}' (type attendu: {expected_type}) ne doit pas être nulle. Valeur: "),
                                F.col(f"{cname}_raw").cast("string"),
                                F.lit("")
                            ).alias("error_message"),
                            F.col(f"{cname}_raw").cast("string").alias("raw_value"),
                            F.lit(1).alias("error_count"),
                            F.col("line_id").alias("line_id"),
                        )
                    )
                    errors_list.append(err)
                    column_errors.setdefault(cname, []).append(f"{null_count} valeurs nulles")
            
            # La colonne validée = cast sécurisé
            validated_columns[cname] = col_test

        # Reconstruire DataFrame (ZÉRO DOUBLON)
        df_final = df.select(*[validated_columns[c].alias(c) for c in validated_columns.keys()])

        print(f"\n✅ DataFrame final : {len(df_final.columns)} colonnes uniques")

        if column_errors:
            print("\n⚠️ Résumé erreurs par colonne :")
            for col_name, errors_list_col in column_errors.items():
                print(f"  - {col_name}: {', '.join(errors_list_col)}")

        return df_final, errors_list, column_errors

    # ==================== CHECK DATA QUALITY ====================

    def check_data_quality(self, df: DataFrame, table_name: str, merge_keys: list,
                           filename: str = None, column_defs=None) -> DataFrame:
        """Vérifie doublons uniquement (pas nulls)"""

        if "line_id" in df.columns:
            df = df.drop("line_id")

        df = df.withColumn("line_id", F.row_number().over(
            Window.orderBy(F.monotonically_increasing_id())
        ))

        errors_df = self.spark.createDataFrame([], self.ERROR_SCHEMA)

        data_columns = [c for c in df.columns if c not in
                        ["line_id", "yyyy", "mm", "dd", "FILE_PROCESS_DATE", "FILE_NAME_RECEIVED"]]

        if not data_columns:
            return errors_df

        all_null = all(df.filter(F.col(c).isNotNull()).count() == 0 for c in data_columns)
        if all_null:
            return self.spark.createDataFrame(
                [(table_name, filename, "ALL_COLUMNS", "FILE_EMPTY", None, 1)],
                self.ERROR_SCHEMA
            )

        # Doublons
        if merge_keys:
            valid_keys = [k for k in merge_keys if k in df.columns]
            if valid_keys:
                dup_df = (
                    df.groupBy(*valid_keys).count().filter(F.col("count") > 1)
                    .select(
                        F.lit(table_name).alias("table_name"),
                        F.lit(filename).alias("filename"),
                        F.lit(','.join(valid_keys)).alias("column_name"),
                        F.lit("DUPLICATE_KEY").alias("error_message"),
                        F.lit(None).cast("string").alias("raw_value"),
                        F.col("count").alias("error_count")
                    )
                )
                errors_df = errors_df.union(dup_df)

        return errors_df
"""
file_processor.py
Gestion du traitement des fichiers
"""

from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class FileProcessor:
    """Processeur de fichiers"""
    
    def __init__(self, spark):
        self.spark = spark
    
    def check_corrupt_records(self, df: DataFrame, total_rows: int, 
                             tolerance: float, table_name: str, 
                             filename: str) -> tuple:
        """
        Vérifie les enregistrements corrompus
        
        Args:
            df: DataFrame à vérifier
            total_rows: Nombre total de lignes
            tolerance: Tolérance (ratio 0-1 ou nombre absolu)
            table_name: Nom de la table
            filename: Nom du fichier
            
        Returns:
            tuple: (df_cleaned, corrupt_count, should_abort)
                - df_cleaned: DataFrame nettoyé (sans lignes corrompues)
                - corrupt_count: Nombre de lignes corrompues
                - should_abort: True si doit abandonner le traitement
        """
        
        # ✅ Vérifier si la colonne _corrupt_record existe
        if "_corrupt_record" not in df.columns:
            # Pas de colonne = pas d'enregistrements corrompus
            print(f"✅ Aucun enregistrement corrompu détecté")
            return df, 0, False
        
        # Compter les lignes corrompues
        try:
            corrupt_rows = df.filter(F.col("_corrupt_record").isNotNull()).count()
        except Exception as e:
            print(f"⚠️  Erreur lors de la vérification des corruptions : {e}")
            return df, 0, False
        
        if corrupt_rows == 0:
            print(f"✅ Aucun enregistrement corrompu")
            # Supprimer la colonne _corrupt_record si elle existe
            df_clean = df.drop("_corrupt_record")
            return df_clean, 0, False
        
        # Il y a des enregistrements corrompus
        corrupt_ratio = corrupt_rows / total_rows if total_rows > 0 else 0
        
        print(f"⚠️  {corrupt_rows} enregistrement(s) corrompu(s) détecté(s)")
        print(f"   Ratio : {corrupt_ratio*100:.2f}% ({corrupt_rows}/{total_rows})")
        
        # Vérifier la tolérance
        if corrupt_ratio > tolerance:
            print(f"❌ Tolérance dépassée !")
            print(f"   Tolérance configurée : {tolerance*100:.2f}%")
            print(f"   Ratio actuel : {corrupt_ratio*100:.2f}%")
            return df, corrupt_rows, True  # should_abort = True
        else:
            print(f"✅ Tolérance OK ({tolerance*100:.2f}%)")
            print(f"   → Filtrage des {corrupt_rows} ligne(s) corrompue(s)")
            
            # Filtrer les lignes corrompues
            df_clean = df.filter(F.col("_corrupt_record").isNull()).drop("_corrupt_record")
            
            clean_count = df_clean.count()
            print(f"   → {clean_count} ligne(s) valides conservées")
            
            return df_clean, corrupt_rows, False
    
    def add_processing_metadata(self, df: DataFrame, filename: str) -> DataFrame:
        """
        Ajoute métadonnées de traitement
        
        Args:
            df: DataFrame source
            filename: Nom du fichier
            
        Returns:
            DataFrame avec métadonnées
        """
        
        now = datetime.now()
        
        df_with_meta = (
            df
            .withColumn("FILE_NAME_RECEIVED", F.lit(filename))
            .withColumn("FILE_PROCESS_DATE", F.lit(now))
            .withColumn("PROCESSING_TIMESTAMP", F.current_timestamp())
        )
        
        return df_with_meta
    
    def validate_row_count(self, df: DataFrame, min_rows: int = 0,
                          max_rows: int = None) -> tuple:
        """
        Valide le nombre de lignes
        
        Args:
            df: DataFrame à valider
            min_rows: Nombre minimum de lignes
            max_rows: Nombre maximum de lignes (optionnel)
            
        Returns:
            tuple: (is_valid, row_count, error_message)
        """
        
        row_count = df.count()
        
        if row_count < min_rows:
            return False, row_count, f"Trop peu de lignes ({row_count} < {min_rows})"
        
        if max_rows is not None and row_count > max_rows:
            return False, row_count, f"Trop de lignes ({row_count} > {max_rows})"
        
        return True, row_count, None
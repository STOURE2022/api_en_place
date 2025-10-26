"""
Configuration Unity Catalog - Pour Pipeline WAX avec Auto Loader
Plus de support DBFS ni Local, Unity Catalog uniquement
Paramètres récupérés depuis API de configuration
"""

from typing import Dict, Any, Optional
import os


class Config:
    """
    Configuration Unity Catalog pour Pipeline WAX
    Fichiers dans Volume, Tables dans Schéma séparé
    Support Auto Loader avec checkpoints et schemas
    
    Paramètres récupérés depuis API ou variables d'environnement
    """

    def __init__(
            self,
            # Unity Catalog - Fichiers
            catalog: str = None,
            schema_files: str = None,
            volume: str = None,

            # Unity Catalog - Tables
            schema_tables: str = None,

            # Paramètres généraux
            env: str = None,
            version: str = None,
            
            # API Configuration
            api_url: str = None,
            use_api: bool = True
    ):
        """
        Configuration Unity Catalog

        Args:
            catalog: Catalogue Unity (ex: "abu_catalog")
            schema_files: Schéma pour fichiers/volume (ex: "databricksassetbundletest")
            volume: Nom du volume (ex: "externalvolumetes")
            schema_tables: Schéma pour tables Delta (ex: "gdp_poc_dev")
            env: Environnement (dev/int/prd)
            version: Version pipeline (v1, v2, etc.)
            api_url: URL de l'API de configuration (ex: http://localhost:5000)
            use_api: Si True, récupère les params depuis l'API
        """
        
        # ========== RÉCUPÉRATION DES PARAMÈTRES ==========
        
        # Environnement par défaut
        target_env = env or os.getenv('PIPELINE_ENV', 'dev')
        
        # Si use_api = True, récupérer depuis l'API
        if use_api:
            params = self._load_from_api(target_env, api_url)
            
            # Si API échoue, fallback vers paramètres fournis ou variables d'environnement
            if params:
                print(f"✅ Configuration chargée depuis API pour env='{target_env}'")
                catalog = params.get('catalog', catalog)
                schema_files = params.get('schema_files', schema_files)
                volume = params.get('volume', volume)
                schema_tables = params.get('schema_tables', schema_tables)
                env = params.get('env', env)
                version = params.get('version', version)
            else:
                print(f"⚠️  API non disponible, utilisation des paramètres fournis ou variables d'environnement")
        
        # Fallback vers variables d'environnement si paramètres non fournis
        self.catalog = catalog or os.getenv('CATALOG', 'abu_catalog')
        self.schema_files = schema_files or os.getenv('SCHEMA_FILES', 'databricksassetbundletest')
        self.volume = volume or os.getenv('VOLUME', 'externalvolumetes')
        self.schema_tables = schema_tables or os.getenv('SCHEMA_TABLES', 'gdp_poc_dev')
        self.env = target_env
        self.version = version or os.getenv('VERSION', 'v1')

        # ========== CHEMINS DE BASE ==========
        self.volume_base = f"/Volumes/{self.catalog}/{self.schema_files}/{self.volume}"

        # ========== RÉPERTOIRES D'ENTRÉE ==========
        self.input_base = f"{self.volume_base}/input"
        
        # ✅ CHANGEMENT 1 : zip_dir (répertoire) au lieu de zip_path (fichier)
        self.zip_dir = f"{self.input_base}/zip"
        
        # Config Excel
        self.config_dir = f"{self.input_base}/config"
        
        # ✅ CHANGEMENT 2 : Nom cohérent avec autoloader_module.py
        self.excel_path = f"{self.config_dir}/wax_config.xlsx"
        
        # ========== RÉPERTOIRES DE TRAVAIL ==========
        # ✅ CHANGEMENT 3 : extracted/ à la racine (pas dans temp/)
        self.extract_dir = f"{self.volume_base}/extracted"
        
        # ✅ NOUVEAU : Checkpoints pour Auto Loader
        self.checkpoint_dir = f"{self.volume_base}/checkpoints"
        
        # ✅ NOUVEAU : Schemas pour Auto Loader
        self.schema_dir = f"{self.volume_base}/schemas"

        # ========== LOGS ==========
        self.log_base = f"{self.volume_base}/logs"
        self.log_exec_path = f"{self.log_base}/execution"
        self.log_quality_path = f"{self.log_base}/quality"
        
        # Bad records
        self.bad_records_base = f"{self.log_base}/badrecords"

        # ========== PATTERNS & MAPPINGS (conservés) ==========
        self.date_patterns = [
            "dd/MM/yyyy HH:mm:ss",
            "dd/MM/yyyy",
            "yyyy-MM-dd HH:mm:ss",
            "yyyy-MM-dd'T'HH:mm:ss",
            "yyyyMMddHHmmss",
            "yyyyMMdd"
        ]

        self.type_mapping = {
            "STRING": "string",
            "INTEGER": "int",
            "INT": "int",
            "LONG": "long",
            "FLOAT": "float",
            "DOUBLE": "double",
            "DECIMAL": "decimal(18,2)",
            "BOOLEAN": "boolean",
            "DATE": "date",
            "TIMESTAMP": "timestamp"
        }

    def _load_from_api(self, env: str, api_url: str = None) -> Optional[Dict]:
        """
        Charge la configuration depuis l'API
        
        Args:
            env: Environnement cible
            api_url: URL de l'API (optionnel)
            
        Returns:
            Dict avec les paramètres ou None si erreur
        """
        try:
            # Import dynamique pour éviter dépendance obligatoire
            from config_client import get_config_from_api
            
            # URL par défaut
            api_url = api_url or os.getenv('CONFIG_API_URL', 'http://localhost:5000')
            
            # Récupérer config
            config = get_config_from_api(env=env, api_url=api_url)
            
            return config
            
        except ImportError:
            print("⚠️  config_client non disponible, config manuelle requise")
            return None
        except Exception as e:
            print(f"⚠️  Erreur récupération config API : {e}")
            return None

    def get_table_full_name(self, table_name: str) -> str:
        """
        Retourne nom complet table Unity Catalog

        Args:
            table_name: Nom simple de la table

        Returns:
            Nom complet: catalog.schema_tables.table_name
        """
        return f"{self.catalog}.{self.schema_tables}.{table_name}"

    def get_bad_records_path(self, table_name: str) -> str:
        """Chemin bad records dans le volume"""
        return f"{self.bad_records_base}/{self.env}/{table_name}"

    def validate_paths(self) -> Dict[str, Any]:
        """
        Valide configuration Unity Catalog
        ✅ NOUVEAU : Crée automatiquement les répertoires manquants
        """
        issues = []

        # Vérifier format chemins
        if not self.volume_base.startswith("/Volumes/"):
            issues.append(f"❌ volume_base doit commencer par /Volumes/")

        # Vérifier existence volume
        if not os.path.exists(self.volume_base):
            issues.append(f"❌ Volume introuvable : {self.volume_base}")
            return {
                "valid": False,
                "issues": issues,
                "mode": "Unity Catalog - Error"
            }

        # Info configuration
        issues.append(f"ℹ️  Configuration Unity Catalog:")
        issues.append(f"   📁 Fichiers: {self.catalog}.{self.schema_files}.{self.volume}")
        issues.append(f"   🗄️  Tables: {self.catalog}.{self.schema_tables}")
        issues.append(f"   📂 Volume: {self.volume_base}")
        issues.append(f"   🌍 Env: {self.env}")

        # ✅ NOUVEAU : Créer répertoires automatiquement
        dirs_to_create = [
            self.input_base,
            self.zip_dir,
            self.config_dir,
            self.extract_dir,
            self.checkpoint_dir,
            self.schema_dir,
            self.log_base,
            self.log_exec_path,
            self.log_quality_path,
            self.bad_records_base
        ]

        for dir_path in dirs_to_create:
            if not os.path.exists(dir_path):
                try:
                    os.makedirs(dir_path, exist_ok=True)
                    issues.append(f"ℹ️  Créé : {dir_path}")
                except Exception as e:
                    issues.append(f"⚠️  Impossible de créer {dir_path}: {e}")

        # Vérifier fichier Excel
        if os.path.exists(self.excel_path):
            issues.append(f"✅ Excel trouvé : {self.excel_path}")
        else:
            issues.append(f"⚠️  Excel manquant : {self.excel_path}")

        return {
            "valid": not any(msg.startswith("❌") for msg in issues),
            "issues": issues,
            "mode": "Unity Catalog - Tables Managées"
        }

    def print_config(self):
        """Affiche configuration"""
        print("=" * 80)
        print("⚙️  CONFIGURATION UNITY CATALOG - WAX PIPELINE")
        print("=" * 80)
        print(f"📚 Catalogue    : {self.catalog}")
        print(f"📂 Schéma Files : {self.schema_files}")
        print(f"💾 Volume       : {self.volume}")
        print(f"🗄️  Schéma Tables: {self.schema_tables}")
        print(f"🌍 Environnement: {self.env}")
        print(f"📌 Version      : {self.version}")
        print()
        print(f"📁 Chemins principaux:")
        print(f"   Base Volume  : {self.volume_base}")
        print(f"   ZIP Dir      : {self.zip_dir}")
        print(f"   Excel        : {self.excel_path}")
        print(f"   Extracted    : {self.extract_dir}")
        print(f"   Checkpoints  : {self.checkpoint_dir}")
        print(f"   Schemas      : {self.schema_dir}")
        print(f"   Logs         : {self.log_base}")
        print("=" * 80)

    def to_dict(self) -> Dict[str, Any]:
        """Convertit en dictionnaire"""
        return {
            "catalog": self.catalog,
            "schema_files": self.schema_files,
            "volume": self.volume,
            "schema_tables": self.schema_tables,
            "env": self.env,
            "version": self.version,
            "volume_base": self.volume_base,
            "zip_dir": self.zip_dir,
            "excel_path": self.excel_path,
            "extract_dir": self.extract_dir,
            "checkpoint_dir": self.checkpoint_dir,
            "schema_dir": self.schema_dir,
            "log_exec_path": self.log_exec_path,
            "log_quality_path": self.log_quality_path
        }

    def __repr__(self) -> str:
        return (
            f"Config(Unity Catalog - WAX Pipeline)\n"
            f"  Fichiers: {self.catalog}.{self.schema_files}.{self.volume}\n"
            f"  Tables: {self.catalog}.{self.schema_tables}\n"
            f"  Env: {self.env}, Version: {self.version}"
        )


# ========== FACTORY FUNCTION ==========

def create_config_from_api(env: str = "dev", api_url: str = None) -> Config:
    """
    Factory function pour créer une Config depuis l'API
    
    Args:
        env: Environnement (dev/int/prd)
        api_url: URL de l'API (optionnel)
        
    Returns:
        Instance de Config
        
    Exemple:
        config = create_config_from_api("dev")
        config = create_config_from_api("prd", "http://prod-api:5000")
    """
    return Config(env=env, api_url=api_url, use_api=True)


def create_config_manual(**kwargs) -> Config:
    """
    Factory function pour créer une Config manuellement
    
    Args:
        **kwargs: Paramètres de configuration
        
    Returns:
        Instance de Config
        
    Exemple:
        config = create_config_manual(
            catalog="my_catalog",
            schema_files="my_schema",
            volume="my_volume",
            schema_tables="my_tables",
            env="dev"
        )
    """
    return Config(use_api=False, **kwargs)


if __name__ == "__main__":
    # Test de la configuration
    print("🧪 TEST DE CONFIGURATION\n")
    
    # Test 1 : Configuration depuis API
    print("1️⃣  Test Config depuis API (dev):")
    try:
        config_api = create_config_from_api("dev")
        config_api.print_config()
    except Exception as e:
        print(f"❌ Erreur : {e}")
    
    print("\n" + "=" * 80 + "\n")
    
    # Test 2 : Configuration manuelle
    print("2️⃣  Test Config manuelle:")
    config_manual = create_config_manual(
        catalog="test_catalog",
        schema_files="test_schema",
        volume="test_volume",
        schema_tables="test_tables",
        env="test"
    )
    config_manual.print_config()

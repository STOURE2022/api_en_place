"""
WAX Data Ingestion Pipeline
Module d'ingestion de données avec validation qualité et logging
"""

__version__ = "2.0.0"
__author__ = "WAX Team"

from .unzip_module  import main as unzip_module
from .autoloader_module import main as autoloader_module
from .main import main as waxng_ingestion


# Imports corrigés (. au lieu de src.)
from .config import Config
from .validator import DataValidator
from .file_processor import FileProcessor
from .delta_manager import DeltaManager
from .logger_manager import LoggerManager
from .ingestion import IngestionManager
from .column_processor import ColumnProcessor

__all__ = [
    'Config',
    'DataValidator',
    'FileProcessor',
    'DeltaManager',
    'LoggerManager',
    'IngestionManager',
    'ColumnProcessor',
    'Environment',
    'ConfigFactory',
    'unzip_module',
    'autoloader_module',
    'waxng_ingestion'
]
"""
config_api.py
API REST pour servir les paramètres de configuration WAX
"""

from flask import Flask, jsonify, request
import json
import os
from datetime import datetime

app = Flask(__name__)

# Chemin vers le fichier de configuration
# Pour Databricks : /dbfs/Volumes/catalog/schema/volume/config/config_params.json
CONFIG_FILE = os.getenv('CONFIG_FILE_PATH', '/dbfs/Volumes/abu_catalog/databricksassetbundletest/externalvolumetes/input/config/config_params.json')

# Fallback si pas de fichier
DEFAULT_CONFIG = {
    "dev": {
        "catalog": "abu_catalog",
        "schema_files": "databricksassetbundletest",
        "volume": "externalvolumetes",
        "schema_tables": "gdp_poc_dev",
        "env": "dev",
        "version": "v1"
    },
    "int": {
        "catalog": "abu_catalog",
        "schema_files": "databricksassetbundletest",
        "volume": "externalvolumetes",
        "schema_tables": "gdp_poc_int",
        "env": "int",
        "version": "v1"
    },
    "prd": {
        "catalog": "abu_catalog",
        "schema_files": "databricksassetbundletest_prd",
        "volume": "externalvolumetes_prd",
        "schema_tables": "gdp_poc_prd",
        "env": "prd",
        "version": "v1"
    }
}


def load_config():
    """Charge la configuration depuis le fichier JSON"""
    try:
        if os.path.exists(CONFIG_FILE):
            with open(CONFIG_FILE, 'r') as f:
                return json.load(f)
        else:
            print(f"⚠️  Fichier config non trouvé : {CONFIG_FILE}")
            print(f"📝 Utilisation de la config par défaut")
            return DEFAULT_CONFIG
    except Exception as e:
        print(f"❌ Erreur lecture config : {e}")
        return DEFAULT_CONFIG


def save_config(config_data):
    """Sauvegarde la configuration dans le fichier JSON"""
    try:
        # Créer le répertoire si nécessaire
        os.makedirs(os.path.dirname(CONFIG_FILE), exist_ok=True)
        
        with open(CONFIG_FILE, 'w') as f:
            json.dump(config_data, f, indent=2)
        return True
    except Exception as e:
        print(f"❌ Erreur sauvegarde config : {e}")
        return False


@app.route('/health', methods=['GET'])
def health():
    """Health check"""
    return jsonify({
        "status": "healthy",
        "service": "WAX Config API",
        "timestamp": datetime.now().isoformat(),
        "config_file": CONFIG_FILE,
        "config_exists": os.path.exists(CONFIG_FILE)
    }), 200


@app.route('/config', methods=['GET'])
def get_all_configs():
    """
    Récupère toutes les configurations
    
    GET /config
    """
    config = load_config()
    return jsonify({
        "status": "success",
        "data": config,
        "timestamp": datetime.now().isoformat()
    }), 200


@app.route('/config/<env>', methods=['GET'])
def get_config(env):
    """
    Récupère la configuration pour un environnement
    
    GET /config/dev
    GET /config/int
    GET /config/prd
    """
    config = load_config()
    
    if env not in config:
        return jsonify({
            "status": "error",
            "message": f"Environment '{env}' not found",
            "available_environments": list(config.keys())
        }), 404
    
    return jsonify({
        "status": "success",
        "environment": env,
        "data": config[env],
        "timestamp": datetime.now().isoformat()
    }), 200


@app.route('/config/<env>', methods=['POST'])
def create_or_update_config(env):
    """
    Crée ou met à jour la configuration pour un environnement
    
    POST /config/dev
    Body: {
        "catalog": "abu_catalog",
        "schema_files": "databricksassetbundletest",
        "volume": "externalvolumetes",
        "schema_tables": "gdp_poc_dev",
        "env": "dev",
        "version": "v1"
    }
    """
    if not request.json:
        return jsonify({
            "status": "error",
            "message": "Request body must be JSON"
        }), 400
    
    # Validation des champs requis
    required_fields = ["catalog", "schema_files", "volume", "schema_tables", "env", "version"]
    missing_fields = [f for f in required_fields if f not in request.json]
    
    if missing_fields:
        return jsonify({
            "status": "error",
            "message": f"Missing required fields: {missing_fields}"
        }), 400
    
    # Charger config existante
    config = load_config()
    
    # Mettre à jour
    config[env] = request.json
    
    # Sauvegarder
    if save_config(config):
        return jsonify({
            "status": "success",
            "message": f"Configuration for '{env}' updated successfully",
            "data": config[env],
            "timestamp": datetime.now().isoformat()
        }), 200
    else:
        return jsonify({
            "status": "error",
            "message": "Failed to save configuration"
        }), 500


@app.route('/config/<env>', methods=['DELETE'])
def delete_config(env):
    """
    Supprime la configuration pour un environnement
    
    DELETE /config/dev
    """
    config = load_config()
    
    if env not in config:
        return jsonify({
            "status": "error",
            "message": f"Environment '{env}' not found"
        }), 404
    
    # Supprimer
    del config[env]
    
    # Sauvegarder
    if save_config(config):
        return jsonify({
            "status": "success",
            "message": f"Configuration for '{env}' deleted successfully",
            "timestamp": datetime.now().isoformat()
        }), 200
    else:
        return jsonify({
            "status": "error",
            "message": "Failed to save configuration"
        }), 500


@app.route('/config/<env>/validate', methods=['GET'])
def validate_config(env):
    """
    Valide la configuration pour un environnement
    
    GET /config/dev/validate
    """
    config = load_config()
    
    if env not in config:
        return jsonify({
            "status": "error",
            "message": f"Environment '{env}' not found"
        }), 404
    
    env_config = config[env]
    
    # Validation
    validation_results = {
        "valid": True,
        "issues": [],
        "config": env_config
    }
    
    # Vérifier champs requis
    required_fields = ["catalog", "schema_files", "volume", "schema_tables", "env", "version"]
    for field in required_fields:
        if field not in env_config or not env_config[field]:
            validation_results["valid"] = False
            validation_results["issues"].append(f"Missing or empty field: {field}")
    
    # Vérifier cohérence env
    if env_config.get("env") != env:
        validation_results["valid"] = False
        validation_results["issues"].append(f"Environment mismatch: URL='{env}', config.env='{env_config.get('env')}'")
    
    return jsonify({
        "status": "success" if validation_results["valid"] else "warning",
        "validation": validation_results,
        "timestamp": datetime.now().isoformat()
    }), 200


@app.errorhandler(404)
def not_found(error):
    return jsonify({
        "status": "error",
        "message": "Endpoint not found",
        "available_endpoints": [
            "GET /health",
            "GET /config",
            "GET /config/<env>",
            "POST /config/<env>",
            "DELETE /config/<env>",
            "GET /config/<env>/validate"
        ]
    }), 404


@app.errorhandler(500)
def internal_error(error):
    return jsonify({
        "status": "error",
        "message": "Internal server error",
        "error": str(error)
    }), 500


if __name__ == '__main__':
    # Pour développement local
    print("🚀 Démarrage Config API...")
    print(f"📁 Fichier config : {CONFIG_FILE}")
    
    # Port configurable via variable d'environnement
    port = int(os.getenv('API_PORT', 5000))
    
    app.run(
        host='0.0.0.0',  # Accessible depuis l'extérieur
        port=port,
        debug=True
    )

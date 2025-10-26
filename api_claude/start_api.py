"""
start_api.py
Script pour démarrer l'API de configuration
"""

import os
import sys

# Configurer les variables d'environnement
os.environ['CONFIG_FILE_PATH'] = '/dbfs/Volumes/abu_catalog/databricksassetbundletest/externalvolumetes/input/config/config_params.json'
os.environ['API_PORT'] = '5000'

# Démarrer l'API
if __name__ == '__main__':
    print("🚀 Démarrage de l'API de Configuration WAX")
    print("=" * 80)
    print(f"📁 Fichier config : {os.environ['CONFIG_FILE_PATH']}")
    print(f"🌐 Port          : {os.environ['API_PORT']}")
    print(f"🔗 URL           : http://localhost:{os.environ['API_PORT']}")
    print("=" * 80)
    print("\n✅ API prête pour tests Postman\n")
    
    # Importer et lancer l'API
    from config_api import app
    app.run(
        host='0.0.0.0',
        port=int(os.environ['API_PORT']),
        debug=True
    )

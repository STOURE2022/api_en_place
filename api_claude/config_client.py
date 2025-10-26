"""
config_client.py
Client pour récupérer les paramètres depuis l'API de configuration
"""

import requests
import os
from typing import Dict, Optional
import json


class ConfigClient:
    """Client pour l'API de configuration"""
    
    def __init__(self, api_url: str = None, timeout: int = 5):
        """
        Initialise le client
        
        Args:
            api_url: URL de l'API (ex: http://localhost:5000)
            timeout: Timeout en secondes
        """
        # URL par défaut depuis variable d'environnement
        self.api_url = api_url or os.getenv('CONFIG_API_URL', 'http://localhost:5000')
        self.timeout = timeout
        
        # Enlever le slash final si présent
        self.api_url = self.api_url.rstrip('/')
    
    def health_check(self) -> Dict:
        """
        Vérifie que l'API est disponible
        
        Returns:
            Dict avec status de l'API
        """
        try:
            response = requests.get(
                f"{self.api_url}/health",
                timeout=self.timeout
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            return {
                "status": "error",
                "error": str(e),
                "api_url": self.api_url
            }
    
    def get_config(self, env: str) -> Optional[Dict]:
        """
        Récupère la configuration pour un environnement
        
        Args:
            env: Environnement (dev, int, prd)
            
        Returns:
            Dict avec la configuration ou None si erreur
        """
        try:
            response = requests.get(
                f"{self.api_url}/config/{env}",
                timeout=self.timeout
            )
            response.raise_for_status()
            
            data = response.json()
            if data.get("status") == "success":
                return data.get("data")
            else:
                print(f"❌ Erreur API : {data.get('message')}")
                return None
                
        except requests.exceptions.RequestException as e:
            print(f"❌ Erreur connexion API : {e}")
            return None
    
    def get_all_configs(self) -> Optional[Dict]:
        """
        Récupère toutes les configurations
        
        Returns:
            Dict avec toutes les configurations ou None si erreur
        """
        try:
            response = requests.get(
                f"{self.api_url}/config",
                timeout=self.timeout
            )
            response.raise_for_status()
            
            data = response.json()
            if data.get("status") == "success":
                return data.get("data")
            else:
                return None
                
        except requests.exceptions.RequestException as e:
            print(f"❌ Erreur connexion API : {e}")
            return None
    
    def create_or_update_config(self, env: str, config_data: Dict) -> bool:
        """
        Crée ou met à jour une configuration
        
        Args:
            env: Environnement
            config_data: Données de configuration
            
        Returns:
            True si succès, False sinon
        """
        try:
            response = requests.post(
                f"{self.api_url}/config/{env}",
                json=config_data,
                timeout=self.timeout
            )
            response.raise_for_status()
            
            data = response.json()
            if data.get("status") == "success":
                print(f"✅ Configuration '{env}' mise à jour")
                return True
            else:
                print(f"❌ Erreur : {data.get('message')}")
                return False
                
        except requests.exceptions.RequestException as e:
            print(f"❌ Erreur connexion API : {e}")
            return False
    
    def delete_config(self, env: str) -> bool:
        """
        Supprime une configuration
        
        Args:
            env: Environnement
            
        Returns:
            True si succès, False sinon
        """
        try:
            response = requests.delete(
                f"{self.api_url}/config/{env}",
                timeout=self.timeout
            )
            response.raise_for_status()
            
            data = response.json()
            if data.get("status") == "success":
                print(f"✅ Configuration '{env}' supprimée")
                return True
            else:
                print(f"❌ Erreur : {data.get('message')}")
                return False
                
        except requests.exceptions.RequestException as e:
            print(f"❌ Erreur connexion API : {e}")
            return False
    
    def validate_config(self, env: str) -> Dict:
        """
        Valide une configuration
        
        Args:
            env: Environnement
            
        Returns:
            Dict avec résultat de validation
        """
        try:
            response = requests.get(
                f"{self.api_url}/config/{env}/validate",
                timeout=self.timeout
            )
            response.raise_for_status()
            
            data = response.json()
            return data.get("validation", {})
                
        except requests.exceptions.RequestException as e:
            return {
                "valid": False,
                "issues": [f"Erreur connexion API : {e}"]
            }


# Fonction helper pour utilisation simple
def get_config_from_api(env: str = "dev", api_url: str = None) -> Optional[Dict]:
    """
    Récupère la configuration depuis l'API (fonction helper)
    
    Args:
        env: Environnement
        api_url: URL de l'API (optionnel)
        
    Returns:
        Dict avec la configuration ou None
    """
    client = ConfigClient(api_url=api_url)
    return client.get_config(env)


if __name__ == "__main__":
    # Test du client
    print("🧪 Test du client API")
    print("=" * 60)
    
    # Test 1 : Health check
    client = ConfigClient()
    print("\n1️⃣  Health Check:")
    health = client.health_check()
    print(json.dumps(health, indent=2))
    
    # Test 2 : Récupérer config dev
    print("\n2️⃣  Récupération config DEV:")
    config = client.get_config("dev")
    if config:
        print(json.dumps(config, indent=2))
    else:
        print("❌ Impossible de récupérer la config")
    
    # Test 3 : Valider config
    print("\n3️⃣  Validation config DEV:")
    validation = client.validate_config("dev")
    print(json.dumps(validation, indent=2))
    
    print("\n" + "=" * 60)

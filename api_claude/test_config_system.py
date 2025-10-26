"""
test_config_system.py
Script de test complet pour le système de configuration via API
"""

import sys
import json
import time


def print_section(title):
    """Affiche un titre de section"""
    print("\n" + "=" * 80)
    print(f"  {title}")
    print("=" * 80 + "\n")


def test_api_health(client):
    """Test 1 : Health check"""
    print_section("TEST 1 : Health Check")
    
    health = client.health_check()
    
    if health.get("status") == "healthy":
        print("✅ API opérationnelle")
        print(f"   Service : {health.get('service')}")
        print(f"   Config file : {health.get('config_file')}")
        print(f"   File exists : {health.get('config_exists')}")
        return True
    else:
        print("❌ API non disponible")
        print(f"   Erreur : {health.get('error')}")
        return False


def test_get_all_configs(client):
    """Test 2 : Récupérer toutes les configs"""
    print_section("TEST 2 : Récupération de toutes les configs")
    
    configs = client.get_all_configs()
    
    if configs:
        print(f"✅ {len(configs)} configuration(s) trouvée(s)")
        for env_name in configs.keys():
            print(f"   • {env_name}")
        return True
    else:
        print("❌ Impossible de récupérer les configs")
        return False


def test_get_specific_config(client, env):
    """Test 3 : Récupérer une config spécifique"""
    print_section(f"TEST 3 : Récupération config {env.upper()}")
    
    config = client.get_config(env)
    
    if config:
        print(f"✅ Configuration {env} récupérée")
        print(json.dumps(config, indent=2))
        return True
    else:
        print(f"❌ Config {env} introuvable")
        return False


def test_validate_config(client, env):
    """Test 4 : Valider une config"""
    print_section(f"TEST 4 : Validation config {env.upper()}")
    
    validation = client.validate_config(env)
    
    if validation.get("valid"):
        print(f"✅ Configuration {env} valide")
        if validation.get("issues"):
            print("\n⚠️  Issues :")
            for issue in validation["issues"]:
                print(f"   - {issue}")
        return True
    else:
        print(f"❌ Configuration {env} invalide")
        for issue in validation.get("issues", []):
            print(f"   • {issue}")
        return False


def test_create_update_config(client):
    """Test 5 : Créer/modifier une config"""
    print_section("TEST 5 : Création config TEST")
    
    test_config = {
        "catalog": "test_catalog",
        "schema_files": "test_schema_files",
        "volume": "test_volume",
        "schema_tables": "test_schema_tables",
        "env": "test",
        "version": "v1"
    }
    
    success = client.create_or_update_config("test", test_config)
    
    if success:
        print("✅ Config test créée")
        
        # Vérifier que la config a bien été créée
        config = client.get_config("test")
        if config:
            print("✅ Config test vérifiée")
            print(json.dumps(config, indent=2))
        return True
    else:
        print("❌ Échec création config test")
        return False


def test_delete_config(client):
    """Test 6 : Supprimer une config"""
    print_section("TEST 6 : Suppression config TEST")
    
    success = client.delete_config("test")
    
    if success:
        print("✅ Config test supprimée")
        
        # Vérifier que la config a bien été supprimée
        config = client.get_config("test")
        if config is None:
            print("✅ Suppression confirmée")
        return True
    else:
        print("❌ Échec suppression config test")
        return False


def test_config_class_api_mode():
    """Test 7 : Utilisation de la classe Config avec API"""
    print_section("TEST 7 : Classe Config en mode API")
    
    try:
        from config import create_config_from_api
        
        print("Tentative de création Config depuis API (env=dev)...")
        config = create_config_from_api("dev")
        
        print("\n✅ Config créée depuis API")
        print(f"\n   Catalog       : {config.catalog}")
        print(f"   Schema Files  : {config.schema_files}")
        print(f"   Volume        : {config.volume}")
        print(f"   Schema Tables : {config.schema_tables}")
        print(f"   Env           : {config.env}")
        print(f"   Version       : {config.version}")
        
        return True
        
    except Exception as e:
        print(f"❌ Erreur : {e}")
        import traceback
        traceback.print_exc()
        return False


def test_config_class_manual_mode():
    """Test 8 : Utilisation de la classe Config en mode manuel"""
    print_section("TEST 8 : Classe Config en mode manuel")
    
    try:
        from config import create_config_manual
        
        print("Création Config manuelle...")
        config = create_config_manual(
            catalog="manual_catalog",
            schema_files="manual_schema",
            volume="manual_volume",
            schema_tables="manual_tables",
            env="manual"
        )
        
        print("\n✅ Config créée manuellement")
        print(f"\n   Catalog       : {config.catalog}")
        print(f"   Schema Files  : {config.schema_files}")
        print(f"   Volume        : {config.volume}")
        print(f"   Schema Tables : {config.schema_tables}")
        print(f"   Env           : {config.env}")
        
        return True
        
    except Exception as e:
        print(f"❌ Erreur : {e}")
        import traceback
        traceback.print_exc()
        return False


def run_all_tests(api_url="http://localhost:5000"):
    """Exécute tous les tests"""
    print("\n")
    print("🧪" * 40)
    print("   TESTS DU SYSTÈME DE CONFIGURATION VIA API")
    print("🧪" * 40)
    print(f"\n📡 API URL : {api_url}\n")
    
    # Importer le client
    try:
        from config_client import ConfigClient
    except ImportError:
        print("❌ Impossible d'importer config_client")
        print("   Vérifiez que config_client.py est dans le même répertoire")
        return
    
    # Créer client
    client = ConfigClient(api_url=api_url)
    
    # Tracker les résultats
    results = {}
    
    # Exécuter les tests
    results["health"] = test_api_health(client)
    
    if results["health"]:
        results["get_all"] = test_get_all_configs(client)
        results["get_dev"] = test_get_specific_config(client, "dev")
        results["validate"] = test_validate_config(client, "dev")
        results["create"] = test_create_update_config(client)
        results["delete"] = test_delete_config(client)
    else:
        print("\n⚠️  API non disponible, tests suivants annulés")
        print("\nℹ️  Pour démarrer l'API :")
        print("   python start_api.py")
        return
    
    # Tests Config class (peuvent fonctionner même si API down)
    results["config_api"] = test_config_class_api_mode()
    results["config_manual"] = test_config_class_manual_mode()
    
    # Résumé
    print_section("RÉSUMÉ DES TESTS")
    
    total = len(results)
    passed = sum(1 for v in results.values() if v)
    failed = total - passed
    
    print(f"Total : {total} tests")
    print(f"✅ Réussis : {passed}")
    print(f"❌ Échoués : {failed}")
    
    if failed == 0:
        print("\n🎉 TOUS LES TESTS SONT PASSÉS !")
    else:
        print(f"\n⚠️  {failed} test(s) ont échoué")
    
    print("\n" + "=" * 80 + "\n")
    
    # Détail des résultats
    print("Détail :")
    for test_name, result in results.items():
        status = "✅" if result else "❌"
        print(f"  {status} {test_name}")
    
    print("\n")


if __name__ == "__main__":
    # Récupérer URL API depuis arguments ou utiliser défaut
    import sys
    
    api_url = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:5000"
    
    run_all_tests(api_url)

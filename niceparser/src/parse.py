import time
import sys
import signal

from mhinstore import MhinStore
from config import Config
from protocol import process_block_unified
from fetcher import RSFetcher
from shutdownmanager import ShutdownManager

def parse():
    """
    Fonction principale pour l'analyse des blocs avec une gestion améliorée des interruptions.
    """
    # Configurer le gestionnaire d'arrêt avec un timeout global de 30 secondes
    shutdown_mgr = ShutdownManager(timeout_global=30)
    
    try:
        # Initialize the unified system for storage and indexing
        Config().set_network("mainnet")
        mhin_store = MhinStore(base_path=Config()["BALANCES_STORE"])

        # Get the current height
        current_height = mhin_store.get_last_indexed_block()

        # If the system has already processed blocks, continue from there
        if current_height > 0:
            print(f"Reprise depuis le bloc sauvegardé: {current_height}")
        # Otherwise, use the configured starting height
        else:
            current_height = Config()["START_HEIGHT"]
            print(f"Démarrage depuis le bloc configuré: {current_height}")

        # Initialize the block fetcher
        block_fetcher = RSFetcher(current_height + 1)
        
        # Enregistrer les ressources à fermer avec la méthode correcte pour chaque ressource
        shutdown_mgr.register_resource(block_fetcher, "Block Fetcher", close_method="stop", timeout=10, priority=1)
        shutdown_mgr.register_resource(mhin_store, "MhinStore", close_method="close", timeout=20, priority=2)

        # Variables for performance tracking
        start_time_1000 = time.time()
        ellapsed_1000 = 0
        counter = 0

        try:
            while not shutdown_mgr.is_shutdown_requested():
                start_time = time.time()
                
                # Utiliser un timeout court pour réagir rapidement aux demandes d'arrêt
                block = block_fetcher.get_next_block(timeout=0.5)

                # If no block is available, check for shutdown and wait
                if block is None:
                    if shutdown_mgr.is_shutdown_requested():
                        break
                    time.sleep(0.2)  # Temps d'attente plus court pour mieux réagir
                    continue

                # Check if we have missed blocks
                if block["height"] > current_height + 1:
                    raise Exception(
                        f"Block {block['height']} is ahead of current height {current_height}"
                    )

                # Handling blockchain reorganizations
                while block["height"] < current_height + 1:
                    print(f"Reorganization detected, rolling back block {current_height}")
                    mhin_store.rollback_block(current_height)
                    current_height -= 1

                # Verify height consistency
                assert block["height"] == current_height + 1
                current_height = block["height"]

                # Process the block
                process_block_unified(block, mhin_store)

                # Update performance statistics
                counter += 1
                if counter == 1000:
                    counter = 0
                    ellapsed_1000 = time.time() - start_time_1000
                    start_time_1000 = time.time()

                ellapsed = time.time() - start_time

                # Display progress
                print(
                    f"Block {block['height']} ({ellapsed:.2f}s) ({ellapsed_1000:.2f}s/1000) (last indexed: {mhin_store.get_last_indexed_block()})",
                    end="\r",
                )
                
                # Vérifier périodiquement si un arrêt est demandé (chaque bloc)
                if shutdown_mgr.is_shutdown_requested():
                    break

        except KeyboardInterrupt:
            # Cette section ne devrait normalement plus être atteinte grâce au gestionnaire de signal,
            # mais on la garde comme sécurité
            print("\nInterruption détectée dans la boucle principale")
            # Le bloc finally s'occupera du nettoyage
        except Exception as e:
            print(f"\nUne erreur s'est produite: {e}")
            import traceback
            traceback.print_exc()
            # Le bloc finally s'occupera du nettoyage
    
    except Exception as e:
        print(f"\nErreur lors de l'initialisation: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        print("\nDémarrage de la procédure d'arrêt...")
        # Le ShutdownManager s'occupe de la fermeture ordonnée des ressources
        shutdown_mgr.shutdown()
        print("Fin du programme")


if __name__ == "__main__":
    # Pour MacOS: contourner le problème de propagation des signaux dans les processus
    if sys.platform == 'darwin':
        # Désactiver la gestion intégrée du CTRL+C dans les sous-processus
        signal.signal(signal.SIGINT, signal.SIG_IGN)
    
    parse()
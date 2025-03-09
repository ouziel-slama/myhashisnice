import signal
import threading
import time
import os
import traceback
import multiprocessing
from concurrent.futures import ThreadPoolExecutor, TimeoutError, as_completed

class ShutdownManager:
    """
    Gestionnaire de shutdown pour assurer une fermeture propre et sans blocage
    des ressources lors d'un signal d'interruption.
    """
    
    def __init__(self, timeout_global=30):
        """
        Initialise le gestionnaire de shutdown.
        
        Args:
            timeout_global (int): Délai maximum en secondes avant de forcer l'arrêt
        """
        self.shutdown_in_progress = False
        self.force_shutdown = False
        self.shutdown_start_time = None
        self.timeout_global = timeout_global
        self.resources = []
        self.shutdown_completed = False
        self.orig_sigint_handler = None
        
        # Timer pour le timeout global
        self.shutdown_timer = None
        
        # Installer le gestionnaire de signal
        # Conserver le gestionnaire original pour le restaurer si nécessaire
        self.orig_sigint_handler = signal.getsignal(signal.SIGINT)
        signal.signal(signal.SIGINT, self._handle_sigint)
        # Capture également SIGTERM pour les environnements qui envoient ce signal
        signal.signal(signal.SIGTERM, self._handle_sigint)
    
    def register_resource(self, resource, name, close_method='close', timeout=10, priority=1):
        """
        Enregistre une ressource à fermer lors du shutdown.
        
        Args:
            resource: L'objet ressource à fermer
            name (str): Nom de la ressource pour le logging
            close_method (str): Nom de la méthode à appeler pour fermer la ressource
            timeout (int): Temps maximum en secondes pour fermer cette ressource
            priority (int): Priorité de fermeture (1=haute, 3=basse)
        """
        # Vérifier que la méthode existe
        if not hasattr(resource, close_method):
            print(f"⚠️ Avertissement: {name} n'a pas de méthode '{close_method}'. "
                  f"Méthodes disponibles: {', '.join(method for method in dir(resource) if callable(getattr(resource, method)) and not method.startswith('_'))}")
        
        self.resources.append({
            'resource': resource,
            'name': name,
            'close_method': close_method,
            'timeout': timeout,
            'priority': priority
        })
    
    def _handle_sigint(self, sig, frame):
        """
        Gestionnaire de signal pour SIGINT (Ctrl+C) et SIGTERM.
        """
        if self.force_shutdown:
            print("\nArrêt forcé immédiat! Certaines ressources pourraient ne pas être correctement fermées.")
            
            # Restaurer le gestionnaire de signal original avant de quitter
            if self.orig_sigint_handler:
                signal.signal(signal.SIGINT, self.orig_sigint_handler)
                
            # Tuer brutalement le processus
            os._exit(1)
            
        if self.shutdown_in_progress:
            print("\nSecond signal reçu. Activation de l'arrêt forcé...")
            self.force_shutdown = True
            
            # Restaurer le gestionnaire de signal original pour permettre un 3ème Ctrl+C pour force-kill
            if self.orig_sigint_handler:
                signal.signal(signal.SIGINT, self.orig_sigint_handler)
            
            # Donner 3 secondes supplémentaires avant de forcer l'arrêt
            threading.Timer(3, lambda: os._exit(1) if not self.shutdown_completed else None).start()
            return
            
        # Démarrer le processus de shutdown
        self.shutdown_in_progress = True
        self.shutdown_start_time = time.time()
        
        print("\nSignal d'interruption reçu. Arrêt gracieux en cours, veuillez patienter...")
        
        # Configurer le timer de timeout global
        self.shutdown_timer = threading.Timer(self.timeout_global, self._force_exit)
        self.shutdown_timer.daemon = True
        self.shutdown_timer.start()
        
        # Le shutdown réel sera géré par l'appelant qui vérifiera is_shutdown_requested()
    
    def _force_exit(self):
        """
        Force l'arrêt du programme après le timeout global.
        """
        if not self.shutdown_completed:
            elapsed = time.time() - self.shutdown_start_time
            print(f"\nTimeout de {self.timeout_global}s atteint! L'arrêt a pris {elapsed:.2f}s sans se terminer.")
            print("Arrêt forcé du programme. Certaines ressources peuvent ne pas être correctement fermées.")
            
            # Tentative de dernier recours pour tuer les processus enfants
            self._force_kill_child_processes()
            
            # Restaurer le gestionnaire de signal original avant de quitter
            if self.orig_sigint_handler:
                signal.signal(signal.SIGINT, self.orig_sigint_handler)
                
            os._exit(2)
    
    def _force_kill_child_processes(self):
        """
        Tentative de dernier recours pour tuer tous les processus enfants.
        """
        try:
            import psutil
            current_process = psutil.Process()
            children = current_process.children(recursive=True)
            
            if children:
                print(f"Tentative de tuer {len(children)} processus enfants restants...")
                
                # Tenter de tuer tout le monde ensemble
                for child in children:
                    try:
                        child.kill()
                    except:
                        pass
                
                # Attendre un peu que tout se termine
                gone, still_alive = psutil.wait_procs(children, timeout=1)
                if still_alive:
                    print(f"Impossible de tuer {len(still_alive)} processus enfants")
        except:
            # Si psutil n'est pas disponible ou une autre erreur survient, utiliser
            # les méthodes de multiprocessing directement
            try:
                active_children = multiprocessing.active_children()
                for child in active_children:
                    try:
                        child.terminate()
                    except:
                        pass
            except:
                pass  # Silencer l'erreur si ça échoue aussi
    
    def is_shutdown_requested(self):
        """
        Vérifie si un shutdown a été demandé.
        
        Returns:
            bool: True si le shutdown est en cours, False sinon
        """
        return self.shutdown_in_progress
    
    def shutdown(self):
        """
        Exécute le processus de shutdown de manière ordonnée.
        """
        if not self.shutdown_in_progress:
            self.shutdown_in_progress = True
            self.shutdown_start_time = time.time()
            print("\nDémarrage de l'arrêt contrôlé...")
        
        # Vérification préalable des méthodes de fermeture
        for resource_info in self.resources:
            name = resource_info['name']
            close_method = resource_info['close_method']
            resource = resource_info['resource']
            
            if not hasattr(resource, close_method):
                print(f"⚠️ La ressource {name} n'a pas de méthode '{close_method}'")
                # Trouver une méthode de fermeture alternative
                possible_methods = ['close', 'stop', 'shutdown', 'terminate', 'exit']
                for method in possible_methods:
                    if hasattr(resource, method) and method != close_method:
                        print(f"Utilisation de la méthode '{method}' à la place de '{close_method}' pour {name}")
                        resource_info['close_method'] = method
                        break
        
        # Trier les ressources par priorité (1=haute, 3=basse)
        resources_by_priority = sorted(self.resources, key=lambda r: r['priority'])
        
        # Grouper les ressources par niveau de priorité
        priority_groups = {}
        for resource in resources_by_priority:
            priority = resource['priority']
            if priority not in priority_groups:
                priority_groups[priority] = []
            priority_groups[priority].append(resource)
        
        # Fermer les ressources en respectant la priorité
        for priority in sorted(priority_groups.keys()):
            resources = priority_groups[priority]
            
            if self.force_shutdown:
                print(f"Fermeture forcée des ressources de priorité {priority}...")
                self._force_close_resources(resources)
            else:
                print(f"Fermeture contrôlée des ressources de priorité {priority}...")
                self._close_resources_with_timeout(resources)
            
            # Petite pause entre les groupes de priorité
            time.sleep(0.1)
        
        # Nettoyer les processus enfants restants comme dernier recours
        try:
            active_children = multiprocessing.active_children()
            if active_children:
                print(f"Il reste {len(active_children)} processus enfants actifs")
                # Forcer la terminaison des processus restants
                for child in active_children:
                    try:
                        print(f"Terminaison forcée du processus {child.name} (PID: {child.pid})")
                        child.terminate()
                        # Attendre un peu pour que le processus se termine
                        child.join(0.5)
                        if child.is_alive():
                            print(f"Le processus {child.name} (PID: {child.pid}) est toujours actif, tentative de kill")
                            if hasattr(child, "kill"):  # Python 3.7+
                                child.kill()
                            else:
                                os.kill(child.pid, signal.SIGKILL)
                    except Exception as e:
                        print(f"Erreur lors de la terminaison du processus {child.name}: {e}")
        except Exception as e:
            print(f"Erreur lors de la vérification des processus enfants: {e}")
        
        self.shutdown_completed = True
        
        # Annuler le timer de timeout global s'il est actif
        if self.shutdown_timer and self.shutdown_timer.is_alive():
            self.shutdown_timer.cancel()
        
        # Restaurer le gestionnaire de signal original
        if self.orig_sigint_handler:
            signal.signal(signal.SIGINT, self.orig_sigint_handler)
        
        elapsed = time.time() - self.shutdown_start_time
        print(f"Arrêt complet terminé en {elapsed:.2f} secondes")
    
    def _close_resources_with_timeout(self, resources):
        """
        Ferme un groupe de ressources en parallèle avec timeout.
        
        Args:
            resources (list): Liste des ressources à fermer
        """
        with ThreadPoolExecutor(max_workers=min(len(resources), 5)) as executor:
            future_to_resource = {
                executor.submit(self._close_single_resource, res): res
                for res in resources
            }
            
            for future in as_completed(future_to_resource):
                resource = future_to_resource[future]
                try:
                    success, elapsed = future.result()
                    if success:
                        print(f"✅ {resource['name']} fermé en {elapsed:.2f}s")
                    else:
                        print(f"❌ Échec de fermeture de {resource['name']}")
                except Exception as e:
                    print(f"❌ Erreur lors de la fermeture de {resource['name']}: {e}")
    
    def _close_single_resource(self, resource_info):
        """
        Ferme une ressource unique avec timeout.
        
        Args:
            resource_info (dict): Informations sur la ressource à fermer
            
        Returns:
            tuple: (success, elapsed_time)
        """
        resource = resource_info['resource']
        name = resource_info['name']
        close_method = resource_info['close_method']
        timeout = resource_info['timeout']
        
        start_time = time.time()
        
        try:
            # Vérifier encore une fois que la méthode existe
            if not hasattr(resource, close_method):
                print(f"La ressource {name} n'a pas de méthode '{close_method}'")
                return False, time.time() - start_time
            
            # Obtenir la méthode de fermeture
            close_func = getattr(resource, close_method)
            
            # Créer un thread pour exécuter la fermeture avec timeout
            close_thread = threading.Thread(target=close_func)
            close_thread.daemon = True
            close_thread.start()
            
            # Attendre avec timeout
            close_thread.join(timeout)
            
            # Vérifier si la fermeture est terminée
            if close_thread.is_alive():
                print(f"⚠️ Timeout lors de la fermeture de {name} après {timeout}s")
                return False, time.time() - start_time
            
            return True, time.time() - start_time
            
        except Exception as e:
            print(f"Erreur lors de la fermeture de {name}: {e}")
            traceback.print_exc()
            return False, time.time() - start_time
    
    def _force_close_resources(self, resources):
        """
        Force la fermeture des ressources sans attendre les timeouts.
        
        Args:
            resources (list): Liste des ressources à fermer
        """
        for resource_info in resources:
            try:
                name = resource_info['name']
                print(f"Tentative de fermeture forcée de {name}...")
                
                # Tenter d'appeler la méthode de fermeture, mais ne pas attendre
                resource = resource_info['resource']
                close_method = resource_info['close_method']
                
                if not hasattr(resource, close_method):
                    print(f"La ressource {name} n'a pas de méthode '{close_method}'")
                    continue
                    
                close_func = getattr(resource, close_method)
                
                # Exécuter avec un petit timeout
                close_thread = threading.Thread(target=close_func)
                close_thread.daemon = True
                close_thread.start()
                close_thread.join(0.5)  # Attendre très peu de temps
                
            except Exception as e:
                print(f"Impossible de forcer la fermeture de {name}: {e}")
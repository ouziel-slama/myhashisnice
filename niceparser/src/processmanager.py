import os
import signal
import psutil
import time
import threading
from multiprocessing import Process

class ProcessManager:
    """
    Gestionnaire pour surveiller et contrôler les processus enfants.
    Facilite la terminaison propre lors d'un arrêt du programme.
    """
    
    def __init__(self):
        """
        Initialise le gestionnaire de processus.
        """
        self.processes = {}  # {pid: {'process': process_object, 'name': name}}
        self.lock = threading.RLock()  # Verrou pour l'accès concurrent à processes
        
    def register_process(self, process, name):
        """
        Enregistre un processus à surveiller.
        
        Args:
            process (Process): L'objet Process à surveiller
            name (str): Nom du processus pour le logging
        """
        with self.lock:
            if process.pid:
                self.processes[process.pid] = {
                    'process': process,
                    'name': name,
                    'start_time': time.time()
                }
            else:
                # Le processus n'est pas encore démarré, on l'enregistre comme None pour le moment
                self.processes[id(process)] = {
                    'process': process,
                    'name': name,
                    'start_time': time.time()
                }
                
                # Lorsque le processus démarre, mettre à jour avec le vrai PID
                original_start = process.start
                def wrapped_start(*args, **kwargs):
                    result = original_start(*args, **kwargs)
                    with self.lock:
                        # Supprimer l'ancien enregistrement
                        if id(process) in self.processes:
                            self.processes.pop(id(process))
                        # Ajouter le nouveau avec le vrai PID
                        self.processes[process.pid] = {
                            'process': process,
                            'name': name,
                            'start_time': time.time()
                        }
                    return result
                process.start = wrapped_start
    
    def deregister_process(self, process):
        """
        Désenregistre un processus.
        
        Args:
            process: Le processus à désenregistrer (peut être un objet Process ou un PID)
        """
        with self.lock:
            if isinstance(process, Process):
                pid = process.pid
                if pid and pid in self.processes:
                    self.processes.pop(pid)
                elif id(process) in self.processes:
                    self.processes.pop(id(process))
            elif isinstance(process, int) and process in self.processes:
                self.processes.pop(process)
    
    def terminate_all(self, timeout=5.0, force=False):
        """
        Termine tous les processus enregistrés.
        
        Args:
            timeout (float): Temps d'attente en secondes après l'envoi du signal avant de forcer
            force (bool): Si True, utilise SIGKILL au lieu de SIGTERM
            
        Returns:
            dict: Résultats de terminaison {pid: success}
        """
        results = {}
        
        with self.lock:
            processes_to_terminate = list(self.processes.items())
        
        # Première étape: envoyer les signaux de terminaison
        for pid, proc_info in processes_to_terminate:
            process = proc_info['process']
            name = proc_info['name']
            
            try:
                if isinstance(pid, int) and pid > 0:  # PID valide
                    if force:
                        print(f"Envoi de SIGKILL au processus {name} (PID: {pid})")
                        if hasattr(process, 'kill'):
                            process.kill()
                        else:
                            os.kill(pid, signal.SIGKILL)
                    else:
                        print(f"Envoi de SIGTERM au processus {name} (PID: {pid})")
                        if hasattr(process, 'terminate'):
                            process.terminate()
                        else:
                            os.kill(pid, signal.SIGTERM)
                    
                    # Marquer comme en cours de terminaison
                    results[pid] = False
            except Exception as e:
                print(f"Erreur lors de la terminaison du processus {name} (PID: {pid}): {e}")
                results[pid] = False
        
        # Deuxième étape: attendre la fin des processus avec timeout
        start_time = time.time()
        remaining = list(results.keys())
        
        while remaining and (time.time() - start_time < timeout):
            for pid in list(remaining):  # Créer une copie pour pouvoir modifier pendant l'itération
                try:
                    process = self.processes.get(pid, {}).get('process')
                    
                    # Différentes façons de vérifier si le processus est terminé
                    if process and isinstance(process, Process):
                        if not process.is_alive():
                            remaining.remove(pid)
                            results[pid] = True
                            name = self.processes.get(pid, {}).get('name', 'Inconnu')
                            print(f"Processus {name} (PID: {pid}) terminé")
                    else:
                        # Vérifier via psutil si le processus existe encore
                        if pid > 0 and not psutil.pid_exists(pid):
                            remaining.remove(pid)
                            results[pid] = True
                            name = self.processes.get(pid, {}).get('name', 'Inconnu')
                            print(f"Processus {name} (PID: {pid}) terminé")
                except Exception as e:
                    print(f"Erreur lors de la vérification du processus {pid}: {e}")
            
            # Petite pause pour ne pas surcharger le CPU
            if remaining:
                time.sleep(0.1)
        
        # Troisième étape: forcer la terminaison des processus restants
        if remaining:
            print(f"Les processus suivants n'ont pas terminé après {timeout}s, envoi de SIGKILL:")
            for pid in remaining:
                try:
                    name = self.processes.get(pid, {}).get('name', 'Inconnu')
                    print(f"Forçage de la terminaison du processus {name} (PID: {pid})")
                    
                    process = self.processes.get(pid, {}).get('process')
                    if process and hasattr(process, 'kill'):
                        process.kill()
                    elif pid > 0:
                        os.kill(pid, signal.SIGKILL)
                        
                    # Vérifier une dernière fois
                    time.sleep(0.5)
                    if (process and not process.is_alive()) or (pid > 0 and not psutil.pid_exists(pid)):
                        results[pid] = True
                    else:
                        results[pid] = False
                        print(f"⚠️ Impossible de terminer le processus {name} (PID: {pid})")
                except Exception as e:
                    print(f"Erreur lors de la terminaison forcée du processus {pid}: {e}")
                    results[pid] = False
        
        # Nettoyage final
        with self.lock:
            for pid in list(self.processes.keys()):
                if pid in results and results[pid]:
                    self.processes.pop(pid, None)
        
        return results
    
    def check_zombie_processes(self):
        """
        Vérifie et nettoie les processus zombies.
        
        Returns:
            int: Nombre de zombies détectés
        """
        zombie_count = 0
        
        with self.lock:
            pids_to_check = list(self.processes.keys())
        
        for pid in pids_to_check:
            if isinstance(pid, int) and pid > 0:
                try:
                    # Vérifier si le processus est zombie
                    proc = psutil.Process(pid)
                    if proc.status() == psutil.STATUS_ZOMBIE:
                        zombie_count += 1
                        print(f"Processus zombie détecté: {pid}")
                        
                        # Tenter de récolter le zombie
                        try:
                            os.waitpid(pid, os.WNOHANG)
                            self.deregister_process(pid)
                            print(f"Processus zombie {pid} nettoyé")
                        except Exception as e:
                            print(f"Impossible de nettoyer le zombie {pid}: {e}")
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    # Le processus n'existe plus, le supprimer de notre liste
                    self.deregister_process(pid)
        
        return zombie_count
    
    def get_process_stats(self):
        """
        Obtient des statistiques sur les processus enregistrés.
        
        Returns:
            dict: Statistiques sur les processus
        """
        stats = {
            'total': 0,
            'alive': 0,
            'zombie': 0,
            'dead': 0,
            'processes': []
        }
        
        with self.lock:
            for pid, proc_info in self.processes.items():
                process = proc_info['process']
                name = proc_info['name']
                start_time = proc_info['start_time']
                
                stats['total'] += 1
                proc_stat = {
                    'pid': pid,
                    'name': name,
                    'running_time': time.time() - start_time,
                    'status': 'unknown'
                }
                
                try:
                    if isinstance(pid, int) and pid > 0:
                        # Vérifier le statut via psutil
                        if psutil.pid_exists(pid):
                            proc = psutil.Process(pid)
                            proc_stat['status'] = proc.status()
                            
                            if proc.status() == psutil.STATUS_ZOMBIE:
                                stats['zombie'] += 1
                            else:
                                stats['alive'] += 1
                                
                            # Ajouter des infos supplémentaires
                            proc_stat['cpu_percent'] = proc.cpu_percent(interval=0.1)
                            proc_stat['memory_percent'] = proc.memory_percent()
                        else:
                            proc_stat['status'] = 'dead'
                            stats['dead'] += 1
                    elif isinstance(process, Process):
                        # Vérifier via la méthode is_alive
                        if process.is_alive():
                            proc_stat['status'] = 'alive'
                            stats['alive'] += 1
                        else:
                            proc_stat['status'] = 'dead'
                            stats['dead'] += 1
                except Exception as e:
                    proc_stat['status'] = f'error: {str(e)}'
                    stats['dead'] += 1
                
                stats['processes'].append(proc_stat)
        
        return stats

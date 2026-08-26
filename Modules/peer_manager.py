import socket
import threading
import time
from zeroconf import ServiceInfo, ServiceBrowser, ServiceListener, Zeroconf

SERVICE_TYPE = "_lanchat._tcp.local."

class PeerManager(ServiceListener):
    def __init__(self, my_username, my_port):
        self.my_username = my_username
        self.my_port = my_port
        self.my_service_name = f"{self.my_username}.{SERVICE_TYPE}"
        
        self.zeroconf = Zeroconf()
        self.active_peers = {}
        self.peer_lock = threading.Lock() 
        self.running = True
        
        # This will now successfully call the method below
        self._start_advertising()
        
        self.browser = ServiceBrowser(self.zeroconf, SERVICE_TYPE, self)
        
        # Start the background heartbeat monitor
        threading.Thread(target=self._monitor_stale_peers, daemon=True).start()
        print(f"[PeerManager] Started. Heartbeat monitor active.")

    # ==========================================
    # ADVERTISING METHODS (Previously Omitted)
    # ==========================================
    def _get_local_ip(self):
        """Trick to reliably get the local LAN IP address."""
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            s.connect(('10.255.255.255', 1))
            return s.getsockname()[0]
        except Exception:
            return '127.0.0.1'
        finally:
            s.close()

    def _start_advertising(self):
        """Advertises this node's presence on the LAN."""
        properties = {b'username': self.my_username.encode('utf-8')}
        self.my_info = ServiceInfo(
            type_=SERVICE_TYPE,
            name=self.my_service_name,
            addresses=[socket.inet_aton(self._get_local_ip())],
            port=self.my_port,
            properties=properties,
            server=f"{self.my_username}.local."
        )
        self.zeroconf.register_service(self.my_info)

    # ==========================================
    # SERVICE LISTENER CALLBACKS
    # ==========================================
    def add_service(self, zc: Zeroconf, type_: str, name: str):
        if name == self.my_service_name:
            return 
            
        info = zc.get_service_info(type_, name)
        if info and info.addresses:
            ip = socket.inet_ntoa(info.addresses[0])
            port = info.port
            props = {k.decode('utf-8'): v.decode('utf-8') for k, v in info.properties.items()}
            username = props.get('username', name.split('.')[0])
            
            with self.peer_lock:
                if username in self.active_peers:
                    if self.active_peers[username]['ip'] == ip and self.active_peers[username]['port'] == port:
                        return 
                
                self.active_peers[username] = {'ip': ip, 'port': port}
            print(f"\n[+] Discovered/Updated: {username} at {ip}:{port}\n> ", end="")

    def remove_service(self, zc: Zeroconf, type_: str, name: str):
        if name == self.my_service_name:
            return
            
        username = name.split('.')[0]
        with self.peer_lock:
            if username in self.active_peers:
                del self.active_peers[username]
        print(f"\n[-] Disconnected gracefully: {username}\n> ", end="")

    def update_service(self, zc: Zeroconf, type_: str, name: str):
        pass

    # ==========================================
    # HEARTBEAT & UTILITY METHODS
    # ==========================================
    def _monitor_stale_peers(self):
        """Actively checks if peers are still alive every 10 seconds."""
        while self.running:
            time.sleep(10) 
            peers_snapshot = self.get_peer_list()
            
            for username, data in peers_snapshot.items():
                if not self._is_port_open(data['ip'], data['port']):
                    print(f"\n[!] Ghost Peer Detected: {username} dropped offline unexpectedly. Removing.\n> ", end="")
                    with self.peer_lock:
                        if username in self.active_peers:
                            del self.active_peers[username]

    def _is_port_open(self, ip, port):
        """Attempts a fast TCP connection to verify the peer is reachable."""
        try:
            with socket.create_connection((ip, port), timeout=1.5):
                return True
        except Exception:
            return False

    def get_peer_list(self):
        """Returns a safe copy of the active peers."""
        with self.peer_lock:
            return self.active_peers.copy()

    def stop(self):
        """Clean shutdown."""
        self.running = False
        if hasattr(self, 'my_info'):
            self.zeroconf.unregister_service(self.my_info)
        self.zeroconf.close()
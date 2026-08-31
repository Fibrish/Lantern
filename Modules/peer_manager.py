import socket, threading, time, os, json
from zeroconf import ServiceInfo, ServiceBrowser, ServiceListener, Zeroconf

SERVICE_TYPE = "_lanchat._tcp.local."

class PeerManager(ServiceListener):
    def __init__(self, my_username, my_port, my_node_id, my_pub_key):
        self.my_username = my_username
        self.my_port = my_port
        self.my_node_id = my_node_id
        self.my_pub_key = my_pub_key 
        self.my_service_name = f"{self.my_username}.{SERVICE_TYPE}"
        
        # --- Known Peers Storage ---
        self.storage_dir = ".identity"
        self.known_peers_file = os.path.join(self.storage_dir, "known_peers.json")
        self.known_peers = self._load_known_peers()
        
        self.zeroconf = Zeroconf()
        self.active_peers = {}
        self.peer_lock = threading.Lock() 
        self.running = True
        
        self._start_advertising()
        self.browser = ServiceBrowser(self.zeroconf, SERVICE_TYPE, self)
        threading.Thread(target=self._monitor_stale_peers, daemon=True).start()
        print(f"[PeerManager] Started. Node ID: {self.my_node_id[:8]}...")

    # ==========================================
    # KNOWN PEERS DATABASE (NEW)
    # ==========================================
    def _load_known_peers(self):
        """Loads previously verified peers from disk."""
        if os.path.exists(self.known_peers_file):
            with open(self.known_peers_file, 'r') as f:
                try:
                    return json.load(f)
                except json.JSONDecodeError:
                    return {}
        return {}

    def _save_known_peer(self, username, node_id, pub_key):
        """Saves a newly discovered peer to disk."""
        self.known_peers[username] = {'node_id': node_id, 'pub_key': pub_key}
        if not os.path.exists(self.storage_dir):
            os.makedirs(self.storage_dir)
        with open(self.known_peers_file, 'w') as f:
            json.dump(self.known_peers, f, indent=4)

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
        """Advertises presence, Node ID, and Public Key (Chunked)."""
        pub_key_bytes = self.my_pub_key.encode('utf-8')
        
        properties = {
            b'username': self.my_username.encode('utf-8'),
            b'node_id': self.my_node_id.encode('utf-8'),
            # Split the ~450 byte key into 3 chunks to bypass the 255-byte TXT limit
            b'pk1': pub_key_bytes[:200],
            b'pk2': pub_key_bytes[200:400],
            b'pk3': pub_key_bytes[400:]
        }
        
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
    # UPDATED DISCOVERY (CONFLICT DETECTION)
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
            node_id = props.get('node_id', 'Unknown')
            
            # Reassemble the chunks back into the full PEM string
            pub_key = props.get('pk1', '') + props.get('pk2', '') + props.get('pk3', '')
            if not pub_key:
                pub_key = None
            
            with self.peer_lock:
                # --- 1. Identity Conflict Detection ---
                if username in self.known_peers:
                    stored_id = self.known_peers[username]['node_id']
                    if stored_id != node_id:
                        print(f"\n[SECURITY ALERT] Impersonation attempt for '{username}'!")
                        print(f"If {username} recently reinstalled their app, type: /forget {username}")
                        print("Connection dropped.\n> ", end="")
                        return
                else:
                    # New peer! Trust on first use and save them.
                    self._save_known_peer(username, node_id, pub_key)

                # --- 2. Handle Duplicates ---
                if username in self.active_peers:
                    if self.active_peers[username]['ip'] == ip and self.active_peers[username]['port'] == port:
                        return 
                
                # --- 3. Save to Active Roster ---
                self.active_peers[username] = {
                    'ip': ip, 
                    'port': port,
                    'node_id': node_id,
                    'pub_key': pub_key
                }
                
            print(f"\n[+] Discovered: {username} (ID: {node_id[:8]}...)\n> ", end="")

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

    def _save_known_peers_to_disk(self):
        """Helper to write the known peers dictionary to the JSON file."""
        if not os.path.exists(self.storage_dir):
            os.makedirs(self.storage_dir)
        with open(self.known_peers_file, 'w') as f:
            json.dump(self.known_peers, f, indent=4)

    def _save_known_peer(self, username, node_id, pub_key):
        self.known_peers[username] = {'node_id': node_id, 'pub_key': pub_key}
        self._save_known_peers_to_disk()

    def forget_peer(self, username):
        """Removes a peer's saved identity, allowing them to reconnect with a new key."""
        with self.peer_lock:
            if username in self.known_peers:
                del self.known_peers[username]
                self._save_known_peers_to_disk()
                
                # Also remove from active roster so they have to be re-discovered
                if username in self.active_peers:
                    del self.active_peers[username]
                return True
        return False
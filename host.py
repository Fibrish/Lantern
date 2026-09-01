import sqlite3
import socket
import threading
import hashlib
from Modules import protocol

DB_FILE = "lan_chat_host.db"
HOST_PORT = 5000

class HostServer:
    def __init__(self):
        self.init_db()
        self.active_clients = {}  # {username: {"ip": ip, "port": port, "socket": sock}}
        self.lock = threading.Lock()
        
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind(('0.0.0.0', HOST_PORT))
        self.server_socket.listen()
        
    def init_db(self):
        """Creates the SQLite database if it doesn't exist."""
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    email TEXT UNIQUE NOT NULL,
                    passkey TEXT NOT NULL,
                    username TEXT UNIQUE NOT NULL
                )
            ''')
            conn.commit()

    def add_user(self, email, passkey, username):
        """Used by the Host Admin to provision accounts."""
        hashed_pk = hashlib.sha256(passkey.encode()).hexdigest()
        try:
            with sqlite3.connect(DB_FILE) as conn:
                cursor = conn.cursor()
                cursor.execute('INSERT INTO users (email, passkey, username) VALUES (?, ?, ?)', 
                               (email, hashed_pk, username))
                conn.commit()
            print(f"[Success] User '{username}' provisioned successfully.")
        except sqlite3.IntegrityError:
            print(f"[Error] Email or Username already exists.")

    def verify_login(self, email, passkey):
        """Checks credentials against the SQLite DB."""
        hashed_pk = hashlib.sha256(passkey.encode()).hexdigest()
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT username FROM users WHERE email=? AND passkey=?', (email, hashed_pk))
            result = cursor.fetchone()
            if result:
                return result[0] # Return the username
        return None

    def broadcast_roster(self):
        """Sends the active peer list to all connected clients."""
        with self.lock:
            roster = {
                user: {"ip": data["ip"], "port": data["port"]} 
                for user, data in self.active_clients.items()
            }
            
            # Send to everyone
            for user, data in list(self.active_clients.items()):
                try:
                    protocol.send_message(data["socket"], protocol.TYPE_ROSTER_UPDATE, roster)
                except Exception:
                    # If socket broke, remove them
                    del self.active_clients[user]

    def handle_client(self, client_socket, addr):
        username = None
        try:
            # Wait for Login Packet
            packet_type, payload = protocol.receive_message(client_socket)
            if packet_type == protocol.TYPE_LOGIN:
                email = payload.get("email")
                passkey = payload.get("passkey")
                client_listening_port = payload.get("listening_port")
                
                username = self.verify_login(email, passkey)
                
                if username:
                    # Login Success
                    protocol.send_message(client_socket, protocol.TYPE_LOGIN_RESP, {"status": "success", "username": username})
                    
                    with self.lock:
                        self.active_clients[username] = {
                            "ip": addr[0], 
                            "port": client_listening_port,
                            "socket": client_socket
                        }
                    print(f"[Login] {username} joined from {addr[0]}:{client_listening_port}")
                    self.broadcast_roster()
                    
                    # Keep connection open to monitor if they disconnect
                    while True:
                        ptype, _ = protocol.receive_message(client_socket)
                        if ptype is None: break # Client disconnected
                else:
                    # Login Failed
                    protocol.send_message(client_socket, protocol.TYPE_LOGIN_RESP, {"status": "fail", "reason": "Invalid credentials"})
                    
        except Exception as e:
            pass
        finally:
            if username:
                with self.lock:
                    if username in self.active_clients:
                        del self.active_clients[username]
                print(f"[Disconnect] {username} left.")
                self.broadcast_roster()
            client_socket.close()

    def run(self):
        print(f"--- Host Server Running on Port {HOST_PORT} ---")
        while True:
            client_sock, addr = self.server_socket.accept()
            threading.Thread(target=self.handle_client, args=(client_sock, addr), daemon=True).start()

if __name__ == "__main__":
    server = HostServer()
    while True:
        cmd = input("Command (add user / start server): ").strip().lower()
        if cmd == "add user":
            em = input("Email: ")
            pk = input("Passkey: ")
            un = input("Username: ")
            server.add_user(em, pk, un)
        elif cmd == "start server":
            server.run()
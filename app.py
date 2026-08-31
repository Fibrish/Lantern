import os
import socket
import threading
import time
import sys
from Modules import protocol
from Modules.peer_manager import PeerManager
from Modules.identity_manager import IdentityManager

class LANChatApp:
    def __init__(self, username): # We no longer need the user to input a port!
        self.username = username
        self.running = True
        
        # 1. Load identity
        self.identity = IdentityManager()
        self.node_id = self.identity.node_id
        self.pub_key_pem = self.identity.get_public_key_bytes().decode('utf-8')
        
        # 2. Setup the Server Component FIRST and Auto-Assign Port
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # Port 0 tells the Operating System to find and assign any free port
        self.server_socket.bind(('0.0.0.0', 0)) 
        self.server_socket.listen()
        
        # Retrieve the specific port the OS just gave us
        self.port = self.server_socket.getsockname()[1] 
        print(f"[System] Operating System assigned Port: {self.port}")
        
        # 3. Pass the assigned port to the LAN Discovery module
        self.peer_manager = PeerManager(self.username, self.port, self.node_id, self.pub_key_pem)

    # ==========================================
    # SERVER COMPONENT: Receiving Messages
    # ==========================================
    def start_listening(self):
        """Runs on a background thread to accept incoming connections."""
        print(f"[Server] Listening for messages on port {self.port}...")
        while self.running:
            try:
                # Timeout allows the loop to check self.running periodically
                self.server_socket.settimeout(1.0)
                client_socket, addr = self.server_socket.accept()
                
                # Handle the incoming message in a new thread
                threading.Thread(target=self.handle_incoming, args=(client_socket,), daemon=True).start()
            except socket.timeout:
                continue
            except Exception as e:
                if self.running: print(f"[Error] Listener crashed: {e}")
                break

    # ==========================================
    # SERVER COMPONENT (Explicit Rejection/Success)
    # ==========================================
    def handle_incoming(self, client_socket):
        authenticated = False
        peer_username = None
        current_challenge = None
        session_key = None  
        
        while self.running:
            packet_type, payload = protocol.receive_message(client_socket)
            if packet_type is None: break
                
            if not authenticated:
                if packet_type == protocol.TYPE_AUTH_INIT:
                    peer_username = payload.get("username")
                    current_challenge = os.urandom(16).hex()
                    protocol.send_message(client_socket, protocol.TYPE_AUTH_CHALLENGE, {"nonce": current_challenge})
                
                elif packet_type == protocol.TYPE_AUTH_RESPONSE:
                    claimed_node_id = payload.get("node_id")
                    pub_key = payload.get("pub_key")
                    signature = payload.get("signature")
                    enc_session_key = payload.get("session_key")
                    
                    if not IdentityManager.verify_node_id(pub_key, claimed_node_id):
                        protocol.send_message(client_socket, protocol.TYPE_AUTH_ERROR, {"reason": "Fake Node ID"})
                        break
                        
                    if not IdentityManager.verify_signature(pub_key, current_challenge, signature):
                        protocol.send_message(client_socket, protocol.TYPE_AUTH_ERROR, {"reason": "Invalid Signature"})
                        break
                    
                    try:
                        session_key = self.identity.decrypt_session_key(enc_session_key)
                    except Exception:
                        protocol.send_message(client_socket, protocol.TYPE_AUTH_ERROR, {"reason": "Failed to decrypt session key"})
                        break

                    # Success! Save peer and notify the client they are cleared to send data.
                    self.peer_manager._save_known_peer(peer_username, claimed_node_id, pub_key)
                    authenticated = True
                    protocol.send_message(client_socket, protocol.TYPE_AUTH_SUCCESS, {})
                    
            else:
                if packet_type == protocol.TYPE_CHAT:
                    encrypted_data = payload.get("encrypted_data")
                    try:
                        chat_data = IdentityManager.decrypt_payload(session_key, encrypted_data)
                        content = chat_data.get("content", "")
                        print(f"\n[Incoming] {peer_username}: {content}\n> ", end="")
                    except Exception as e:
                        print(f"\n[SECURITY] Message decryption failed: {e}\n> ", end="")
                        
        client_socket.close()

    # ==========================================
    # CLIENT COMPONENT (Encryption)
    # ==========================================
    def send_message(self, target_username, content):
        peers = self.peer_manager.get_peer_list()
        if target_username not in peers:
            print(f"[Error] {target_username} is not online.")
            return

        target_ip = peers[target_username]['ip']
        target_port = peers[target_username]['port']
        target_pub_key = peers[target_username]['pub_key'] # Needed to encrypt the session key

        try:
            out_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            out_socket.settimeout(5.0)
            out_socket.connect((target_ip, target_port))
            
            # STEP 1 & 2: Handshake Init & Challenge
            protocol.send_message(out_socket, protocol.TYPE_AUTH_INIT, {"username": self.username})
            packet_type, payload = protocol.receive_message(out_socket)
            if packet_type != protocol.TYPE_AUTH_CHALLENGE:
                raise Exception("Server refused authentication.")
            
            # STEP 3: Generate Session Key, Encrypt it, and Sign Response
            nonce = payload.get("nonce")
            signature = self.identity.sign_challenge(nonce)
            session_key = IdentityManager.generate_session_key()
            enc_session_key = self.identity.encrypt_session_key(target_pub_key, session_key)
            
            auth_response = {
                "node_id": self.node_id,
                "pub_key": self.pub_key_pem,
                "signature": signature,
                "session_key": enc_session_key 
            }
            protocol.send_message(out_socket, protocol.TYPE_AUTH_RESPONSE, auth_response)
            
            # Wait for Server Verification
            packet_type, payload = protocol.receive_message(out_socket)
            if packet_type == protocol.TYPE_AUTH_ERROR:
                raise Exception(f"Server rejected authentication: {payload.get('reason')}")
            elif packet_type != protocol.TYPE_AUTH_SUCCESS:
                raise Exception("Unexpected response during authentication.")
            
            # Authorized! Encrypt and send the chat payload
            chat_payload = {
                "content": content,
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
            }
            encrypted_b64 = IdentityManager.encrypt_payload(session_key, chat_payload)
            protocol.send_message(out_socket, protocol.TYPE_CHAT, {"encrypted_data": encrypted_b64})
            
            out_socket.close()
            print(f"[Sent to {target_username}]")
            
        except Exception as e:
            print(f"[Error] Connection failed: {e}")

    # ==========================================
    # APPLICATION UI / MAIN LOOP
    # ==========================================
    def run(self):
        # Start the listener server in the background
        threading.Thread(target=self.start_listening, daemon=True).start()
        
        print("\n--- LAN Chat Ready ---")
        print("Commands: '/peers' to see online users, '/quit' to exit.")
        print("To message someone, type: @username Hello there!")
        
        while self.running:
            try:
                user_input = input("> ").strip()
                
                if user_input.lower() == '/quit':
                    self.shutdown()
                elif user_input.lower() == '/peers':
                    self.show_peers()
                elif user_input.startswith('/forget '):
                    target = user_input.split(' ', 1)[1]
                    if self.peer_manager.forget_peer(target):
                        print(f"Forgot identity for {target}. Their new key will be accepted on next discovery.")
                    else:
                        print(f"No saved identity found for {target}.")
                elif user_input:
                    print("Please specify a user with @username")
            except KeyboardInterrupt:
                self.shutdown()

    def show_peers(self):
        peers = self.peer_manager.get_peer_list()
        print(f"\n--- Online Peers ({len(peers)}) ---")
        for name, data in peers.items():
            print(f" - {name} ({data['ip']}:{data['port']})")
        print("------------------------\n")

    def shutdown(self):
        print("\nShutting down...")
        self.running = False
        self.peer_manager.stop()
        self.server_socket.close()
        sys.exit(0)

if __name__ == "__main__":
    my_name = input("Enter your username: ").strip()
    app = LANChatApp(my_name) # Port is now handled automatically
    app.run()
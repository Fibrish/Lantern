from Modules.peer_manager import PeerManager
import Modules.protocol as protocol
import socket
import threading
import time
import sys

class LANChatApp:
    def __init__(self, username, port):
        self.username = username
        self.port = port
        
        # 1. Initialize LAN Discovery
        self.peer_manager = PeerManager(username, port)
        
        # 2. Setup the Server Component (Listening Socket)
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # Allow immediate reuse of the port if the app restarts
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) 
        self.server_socket.bind(('0.0.0.0', self.port))
        self.server_socket.listen()
        
        self.running = True

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

    def handle_incoming(self, client_socket):
        """Processes the received data using our custom protocol."""
        packet_type, payload = protocol.receive_message(client_socket)
        
        if packet_type == protocol.TYPE_CHAT:
            sender = payload.get("sender", "Unknown")
            content = payload.get("content", "")
            print(f"\n[Incoming] {sender}: {content}\n> ", end="")
            
        client_socket.close()

    # ==========================================
    # CLIENT COMPONENT: Sending Messages
    # ==========================================
    def send_message(self, target_username, content):
        """Looks up a peer's IP/Port and sends a protocol-formatted message."""
        peers = self.peer_manager.get_peer_list()
        
        if target_username not in peers:
            print(f"[Error] {target_username} is not online.")
            return

        target_ip = peers[target_username]['ip']
        target_port = peers[target_username]['port']
        
        # Create payload
        payload = {
            "sender": self.username,
            "content": content,
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
        }

        try:
            # 1. Open a temporary client socket to the target peer
            out_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            out_socket.connect((target_ip, target_port))
            
            # 2. Send using the protocol
            protocol.send_message(out_socket, protocol.TYPE_CHAT, payload)
            out_socket.close()
            print(f"[Sent to {target_username}]")
        except Exception as e:
            print(f"[Error] Failed to send message: {e}")

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
                elif user_input.startswith('@'):
                    # Parse "@username message"
                    parts = user_input.split(' ', 1)
                    if len(parts) >= 2:
                        target = parts[0][1:] # Remove the '@'
                        msg = parts[1]
                        self.send_message(target, msg)
                    else:
                        print("Invalid format. Use: @username message")
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
    # Get username and port from terminal to allow multiple tests on one computer
    my_name = input("Enter your username: ").strip()
    my_port = int(input("Enter your port (e.g., 5000, 5001): ").strip())
    
    app = LANChatApp(my_name, my_port)
    app.run()
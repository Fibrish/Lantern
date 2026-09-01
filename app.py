import socket, json, threading, sys, time, random, uuid
from Modules import protocol
from Modules.local_db import LocalDB

class LANClientApp:
    def __init__(self, host_ip, host_port=5000):
        self.host_ip = host_ip
        self.host_port = host_port
        self.username = None
        self.roster = {}
        self.running = True
        
        self.simulate_drop = False  # Toggle for testing network chaos

        self.p2p_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.p2p_socket.bind(('0.0.0.0', 0))
        self.p2p_socket.listen()
        self.my_p2p_port = self.p2p_socket.getsockname()[1]
        
        self.host_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    def login(self, email, password):
        """Connects to the Host and authenticates."""
        try:
            self.host_socket.connect((self.host_ip, self.host_port))
            
            login_payload = {
                "email": email,
                "passkey": password,
                "listening_port": self.my_p2p_port  
            }  
            protocol.send_message(self.host_socket, protocol.TYPE_LOGIN, login_payload)
            
            packet_type, payload = protocol.receive_message(self.host_socket)
            
            if packet_type == protocol.TYPE_LOGIN_RESP and payload.get("status") == "success":
                self.username = payload.get("username")
                print(f"\n[System] Logged in successfully as '{self.username}'!")
                
                # Initialize SQLite Database for this user
                self.db = LocalDB(self.username)
                
                # Start background workers
                threading.Thread(target=self.listen_to_host, daemon=True).start()
                threading.Thread(target=self.listen_for_p2p_chats, daemon=True).start()
                
                return True
            else:
                print(f"\n[Error] Login Failed: {payload.get('reason', 'Unknown')}")
                return False
                
        except Exception as e:
            print(f"[Error] Could not connect to Host at {self.host_ip}: {e}")
            return False

    def listen_to_host(self):
        while self.running:
            packet_type, payload = protocol.receive_message(self.host_socket)
            if packet_type == protocol.TYPE_ROSTER_UPDATE:
                self.roster = payload
                for peer, info in payload.items():
                    self.db.update_peer_cache(peer, info['ip'], info['port'])

    def listen_for_p2p_chats(self):
        while self.running:
            try:
                self.p2p_socket.settimeout(1.0)
                client_sock, addr = self.p2p_socket.accept()
                
                try:
                    packet_type, payload = protocol.receive_message(client_sock)
                    
                    if packet_type == protocol.TYPE_CHAT:
                        # Optional: Chaos simulator test toggle
                        if self.simulate_drop and random.random() < 0.5:
                            client_sock.close()
                            continue

                        sender = payload.get("sender", "Unknown")
                        content = payload.get("content", "")
                        msg_id = payload.get("msg_id")
                        seq_num = payload.get("seq_num")
                        timestamp = payload.get("timestamp")
                        
                        # Send ACK immediately
                        ack_payload = {"msg_id": msg_id, "status": "delivered"}
                        protocol.send_message(client_sock, protocol.TYPE_ACK, ack_payload)
                        client_sock.close()

                        # Crash-safe atomic insert & sequence validation
                        is_in_order = self.db.save_incoming_message(
                            sender, msg_id, seq_num, content, timestamp
                        )
                        
                        if is_in_order:
                            print(f"\n[Incoming] {sender}: {content}\n> ", end="")
                        
                    else:
                        client_sock.close()
                        
                except Exception:
                    client_sock.close()
                    
            except socket.timeout:
                continue
            except Exception:
                pass

    def send_p2p_message(self, target_username, content):
        if target_username not in self.roster:
            print(f"[Error] {target_username} is not online.")
            return

        if target_username == self.username:
            print("[Error] You cannot message yourself.")
            return

        msg_id = uuid.uuid4().hex  
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
        
        chat_payload = {
            "msg_id": msg_id,      
            "sender": self.username,
            "content": content,
            "timestamp": timestamp
        }
        
        # Execute crash-safe transaction
        self.db.save_outgoing_message(
            target_username, msg_id, content, timestamp, chat_payload
        )
        
        # Dispatch background retry thread
        threading.Thread(target=self._send_with_retry, args=(target_username, chat_payload), daemon=True).start()

    def _send_with_retry(self, target_username, chat_payload, max_retries=3):
        msg_id = chat_payload["msg_id"]
        
        for attempt in range(1, max_retries + 1):
            target_info = self.roster.get(target_username)
            if not target_info: 
                return 
                
            try:
                out_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                out_sock.settimeout(2.0) 
                out_sock.connect((target_info["ip"], target_info["port"]))
                
                protocol.send_message(out_sock, protocol.TYPE_CHAT, chat_payload)
                ack_type, ack_payload = protocol.receive_message(out_sock)
                
                if ack_type == protocol.TYPE_ACK and ack_payload.get("msg_id") == msg_id:
                    self.db.update_message_status(msg_id, "DELIVERED")
                    self.db.remove_from_outbox(msg_id)
                    
                    if attempt == 1:
                        print(f"\n[Sent to {target_username}] (✓ Delivered)\n> ", end="")
                    else:
                        print(f"\n[Sent to {target_username}] (✓ Delivered on retry {attempt})\n> ", end="")
                    out_sock.close()
                    return 
                    
                out_sock.close()
            except Exception:
                pass 
                
            time.sleep(1.0) 
            
        # Update database status to FAILED after all retries are exhausted
        self.db.update_message_status(msg_id, "FAILED")
        print(f"\n[Error] Failed to deliver message to {target_username}. Stored in Outbox.\n> ", end="")

    def run(self):
        print("\n--- LAN Chat Ready ---")
        print("Commands: '/peers' online users, '/status' message history, '/quit' to exit.")
        print("To message someone, type: @username Hello there!")
        
        while self.running:
            try:
                user_input = input("> ").strip()
                if user_input.lower() == '/quit':
                    self.shutdown()

                elif user_input.lower() == '/peers':
                    print(f"\n--- Online Peers ({len(self.roster)}) ---")
                    for name in self.roster:
                        print(f" - {name}")
                    print("------------------------\n")

                elif user_input.startswith('@'):
                    parts = user_input.split(' ', 1)
                    if len(parts) >= 2:
                        target = parts[0][1:]
                        self.send_p2p_message(target, parts[1])
                    else:
                        print("Invalid format. Use: @username message")

                elif user_input.lower() == '/status':
                    print("\n--- Message Status (From Database) ---")
                    with self.db.get_connection() as conn:
                        cursor = conn.cursor()
                        cursor.execute("SELECT receiver, status, content FROM messages WHERE direction='OUTGOING'")
                        rows = cursor.fetchall()
                        if not rows:
                            print("No messages sent yet.")
                        for row in rows:
                            print(f"To: {row[0]} | Status: {row[1]} | Content: '{row[2]}'")
                    print("--------------------------------------\n")

                elif user_input.lower() == '/drop':
                    self.simulate_drop = not self.simulate_drop
                    state = "ON" if self.simulate_drop else "OFF"
                    print(f"\n[System] Network Chaos Simulator is now {state}.\n")

            except KeyboardInterrupt:
                self.shutdown()

    def shutdown(self):
        self.running = False
        self.host_socket.close()
        self.p2p_socket.close()
        sys.exit(0)

if __name__ == "__main__":
    host_ip = input("Enter Host IP (e.g. 127.0.0.1 for local testing): ").strip()
    email = input("Email: ").strip()
    passkey = input("Passkey: ").strip()
    
    app = LANClientApp(host_ip)
    if app.login(email, passkey):
        app.run()
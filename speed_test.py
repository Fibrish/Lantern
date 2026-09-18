import socket, json, threading, sys, time, random, uuid, os, uuid, hashlib, base64
from Modules import protocol
from Modules.local_db import LocalDB

class LANClientApp:
    def __init__(self, host_ip, host_port=5000):
        self.host_ip = host_ip
        self.host_port = host_port
        self.username = None
        self.roster = {}
        self.running = True
        self.reconnecting_peers = {}

        self.peer_health = {}   
        self.peer_timeout = 25.0

        self.peer_last_seen = {} 
        self.peer_timeout = 25.0

        self.active_syncs = set()
        self.sync_lock = threading.Lock()

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
                threading.Thread(target=self.heartbeat_loop, daemon=True).start()
                threading.Thread(target=self.auto_reconnect_loop, daemon=True).start()
                
                
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
            
            # --- NEW: Catch the dead server and exit the loop gracefully ---
            if packet_type is None:
                print("\n[System] Lost connection to Host Server! Running in decentralized P2P mode.\n> ", end="")
                break 

            if packet_type == protocol.TYPE_ROSTER_UPDATE:
                old_roster = set(self.roster.keys())
                self.roster = payload
                new_roster = set(self.roster.keys())
                
                for peer, info in payload.items():
                    self.db.update_peer_cache(peer, info['ip'], info['port'])
                
                just_joined = new_roster - old_roster
                for peer in just_joined:
                    if peer != self.username:
                        
                        # --- NEW FIX: Clear them from the reconnect queue! ---
                        if peer in self.reconnecting_peers:
                            del self.reconnecting_peers[peer]
                            print(f"\n[System] {peer} reconnected via Host Server. Stopping backoff loop.\n> ", end="")
                        
                        # Initialize adaptive health profile
                        self.peer_health[peer] = {
                            "last_seen": time.time(),
                            "rtt": 0.0,
                            "interval": 10.0
                        }
                        threading.Thread(target=self.initiate_sync, args=(peer,), daemon=True).start()

    def listen_for_p2p_chats(self):
        while self.running:
            try:
                self.p2p_socket.settimeout(1.0)
                client_sock, addr = self.p2p_socket.accept()
                
                try:
                    packet_type, payload = protocol.receive_message(client_sock)

                    sender = payload.get("sender") if packet_type != protocol.TYPE_CHAT else payload.get("sender", "Unknown")
                    if sender in self.peer_health:
                        self.peer_health[sender]["last_seen"] = time.time()

                    if packet_type == protocol.TYPE_CHAT:
                        sender = payload.get("sender", "Unknown")
                        content = payload.get("content", "")
                        msg_id = payload.get("msg_id")
                        seq_num = payload.get("seq_num")
                        timestamp = payload.get("timestamp")
                        
                        ack_payload = {"msg_id": msg_id, "status": "delivered"}
                        protocol.send_message(client_sock, protocol.TYPE_ACK, ack_payload)
                        client_sock.close()

                        is_in_order = self.db.save_incoming_message(
                            sender, msg_id, seq_num, content, timestamp
                        )
                        
                        if is_in_order:
                            print(f"\n[Incoming] {sender}: {content}\n> ", end="")
                        else:
                            print(f"\n[Warning] Ignored message from {sender} (Seq: {seq_num}). Sequence mismatch.\n> ", end="")

                    # --- NEW: Handle Incoming Sync Requests ---
                    elif packet_type == protocol.TYPE_SYNC_REQ:
                        
                        sender = payload.get("sender")
                        inbound_seq, _ = self.db.get_seq_numbers(sender)
                        
                        resp_payload = {
                            "sender": self.username, 
                            "last_inbound_seq": inbound_seq
                        }
                        protocol.send_message(client_sock, protocol.TYPE_SYNC_RESP, resp_payload)
                        client_sock.close()
                        
                    elif packet_type == protocol.TYPE_PING:
                        protocol.send_message(client_sock, protocol.TYPE_PONG, {"sender": self.username})
                        client_sock.close()

                    # --- NEW: File Request (Receiver Side) ---
                    elif packet_type == protocol.TYPE_FILE_REQ:
                        sender = payload.get("sender")
                        transfer_id = payload.get("transfer_id")
                        file_name = payload.get("file_name")
                        file_size = payload.get("file_size")
                        total_chunks = payload.get("total_chunks")
                        file_hash = payload.get("file_hash")

                        # Isolate downloads to prevent overwriting local files
                        download_dir = f"Downloads_{self.username}"
                        os.makedirs(download_dir, exist_ok=True)
                        file_path = os.path.join(download_dir, file_name)

                        print(f"\n[Incoming File] {sender} is sending '{file_name}' ({file_size} bytes). Saving to {download_dir}...\n> ", end="")

                        self.db.init_file_transfer(
                            transfer_id, sender, "INCOMING", file_name,
                            file_size, total_chunks, file_path, file_hash, "IN_PROGRESS"
                        )

                        # Create an empty file so the chunk writer can safely use seek() later
                        open(file_path, 'wb').close()

                        # Automatically accept the transfer
                        protocol.send_message(client_sock, protocol.TYPE_FILE_RESP, {
                            "transfer_id": transfer_id,
                            "status": "ACCEPTED"
                        })
                        client_sock.close()

                    # --- NEW: File Response (Sender Side) ---
                    elif packet_type == protocol.TYPE_FILE_RESP:
                        transfer_id = payload.get("transfer_id")
                        status = payload.get("status")
                        
                        if status == "ACCEPTED":
                            print(f"\n[System] File request accepted! Starting transmission...\n> ", end="")
                            self.db.update_file_progress(transfer_id, status="IN_PROGRESS")
                            
                            # Retrieve the target username to spin up the transmission thread
                            with self.db.get_connection() as conn:
                                cursor = conn.cursor()
                                cursor.execute("SELECT peer_username FROM file_transfers WHERE transfer_id=?", (transfer_id,))
                                target = cursor.fetchone()[0]
                            
                            threading.Thread(target=self.transmit_file_worker, args=(target, transfer_id), daemon=True).start()
                        else:
                            self.db.update_file_progress(transfer_id, status="REJECTED")
                            print(f"\n[Error] File transfer was rejected.\n> ", end="")
                            
                        client_sock.close()

                    # --- NEW: File Chunk Processing (Receiver Side) ---
                    elif packet_type == protocol.TYPE_FILE_CHUNK:
                        transfer_id = payload.get("transfer_id")
                        chunk_index = payload.get("chunk_index")
                        encoded_data = payload.get("data")
                        raw_bytes = base64.b64decode(encoded_data)
                        
                        state = self.db.get_transfer_state(transfer_id)
                        if state:
                            _, _, filepath, expected_hash, total_chunks = state
                            chunk_size = 1048576 * 2
                            
                            # r+b mode allows writing to specific byte offsets without overwriting the whole file
                            with open(filepath, 'r+b') as f:
                                f.seek(chunk_index * chunk_size)
                                f.write(raw_bytes)
                                
                            self.db.update_file_progress(transfer_id, chunk_index=chunk_index)
                            
                            # Send ACK so the sender knows it is safe to transmit the next chunk
                            protocol.send_message(client_sock, protocol.TYPE_FILE_ACK, {"transfer_id": transfer_id})
                            
                            # Verify SHA-256 Hash on the final chunk
                            if chunk_index == total_chunks - 1:
                                final_hash = self.get_file_hash(filepath)
                                if final_hash == expected_hash:
                                    self.db.update_file_progress(transfer_id, status="COMPLETED")
                                    print(f"\n[System] File received successfully and verified! Saved to {filepath}\n> ", end="")
                                else:
                                    self.db.update_file_progress(transfer_id, status="HASH_MISMATCH")
                                    print(f"\n[Warning] File received but SHA-256 hash MISMATCH. File corrupted during transit.\n> ", end="")
                                    
                        client_sock.close()

                    else:
                        client_sock.close()
                        
                except Exception:
                    client_sock.close()
                    
            except socket.timeout:
                continue
            except Exception:
                pass

    def send_p2p_message(self, target_username, content):
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
        
        # 1. Let the DB generate the sequence number and save it
        outbound_seq = self.db.save_outgoing_message(
            target_username, msg_id, content, timestamp, chat_payload
        )
        
        # 2. Inform the user if the message is being queued for offline delivery
        if target_username not in self.roster:
            print(f"\n[System] {target_username} is offline. Message queued in Outbox.\n> ", end="")
        
        threading.Thread(target=self._send_with_retry, args=(target_username, chat_payload), daemon=True).start()

    def _send_with_retry(self, target_username, chat_payload, max_retries=3):
        msg_id = chat_payload["msg_id"]
        
        # If we already know they are offline, skip the loop and mark as FAILED
        if target_username not in self.roster:
            self.db.update_message_status(msg_id, "FAILED")
            return

        for attempt in range(1, max_retries + 1):
            target_info = self.roster.get(target_username)
            if not target_info: 
                break  # Peer went offline during retries, break out of loop
                
            try:
                out_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                out_sock.settimeout(2.0) 
                out_sock.connect((target_info["ip"], int(target_info["port"])))
                
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
            
        # Update database status to FAILED after all retries exhaust or if they disconnect
        self.db.update_message_status(msg_id, "FAILED")
        print(f"\n[Error] Failed to deliver message to {target_username}. Stored in Outbox.\n> ", end="")

    def run(self):
        # --- CLI Welcome Banner ---
        print(f"\n{'='*45}")
        print(f" LAN Chat CLI - Active Node: {self.username}")
        print(f" P2P Listening Port: {self.my_p2p_port}")
        print(f"{'='*45}")
        print(" Commands:")
        print("   /info      - Show local node ID & connection status")
        print("   /peers     - Show roster (Online/Offline status)")
        print("   /status    - Show outgoing message delivery states")
        print("   /metrics   - View adaptive heartbeat network stats")
        print("   /sendfile  - Send a file (Usage: /sendfile username filepath)")
        print("   /pause     - Pause transfer (Usage: /pause transfer_id)")
        print("   /resume    - Resume transfer (Usage: /resume transfer_id)")
        print("   /quit      - Exit application")
        print(" Messaging:")
        print("   @username Your message here")
        print(f"{'='*45}\n")
        
        while self.running:
            try:
                user_input = input("> ").strip()
                if not user_input:
                    continue

                if user_input.lower() == '/quit':
                    self.shutdown()

                # --- 1. Local Node ID & Connection Status ---
                elif user_input.lower() == '/info':
                    print("\n--- Local Node Identity ---")
                    print(f" Username:  {self.username}")
                    print(f" P2P Port:  {self.my_p2p_port}")
                    print(f" Host IP:   {self.host_ip}:{self.host_port}")
                    
                    try:
                        host_status = "Connected (Online)" if self.host_socket.fileno() != -1 else "Disconnected (Decentralized P2P Mode)"
                    except Exception:
                        host_status = "Disconnected (Decentralized P2P Mode)"
                        
                    print(f" Status:    {host_status}")
                    print("---------------------------\n")

                # --- 2. Discovered Peer Roster & Status ---
                elif user_input.lower() == '/peers':
                    print("\n--- Network Roster ---")
                    
                    print("[ ONLINE PEERS ]")
                    online_count = 0
                    for name in self.roster:
                        if name == self.username: continue
                        online_count += 1
                        health = self.peer_health.get(name, {})
                        rtt_ms = health.get("rtt", 0.0) * 1000
                        print(f"  🟢 {name} (Ping: {rtt_ms:.1f}ms)")
                    
                    if online_count == 0:
                        print("  (No one is online right now)")

                    print("\n[ OFFLINE / RECONNECTING ]")
                    offline_count = 0
                    for name, info in self.reconnecting_peers.items():
                        offline_count += 1
                        print(f"  🔴 {name} (Auto-reconnect attempt {info['attempts']}...)")
                        
                    if offline_count == 0:
                        print("  (No peers are currently disconnected)")
                        
                    print("----------------------\n")

                # --- 3. Send Messages from CLI ---
                elif user_input.startswith('@'):
                    parts = user_input.split(' ', 1)
                    if len(parts) >= 2:
                        target = parts[0][1:]
                        message_content = parts[1]
                        self.send_p2p_message(target, message_content)
                    else:
                        print("[Error] Invalid format. Use: @username message")

                # --- 4. Display Delivery Status ---
                elif user_input.lower() == '/status':
                    print("\n--- Message Delivery Status ---")
                    with self.db.get_connection() as conn:
                        cursor = conn.cursor()
                        cursor.execute("SELECT receiver, status, content, timestamp FROM messages WHERE direction='OUTGOING' ORDER BY timestamp DESC LIMIT 10")
                        rows = cursor.fetchall()
                        if not rows:
                            print(" No messages sent yet.")
                        for row in rows:
                            preview = row[2][:20] + "..." if len(row[2]) > 20 else row[2]
                            print(f" [{row[3]}] To: {row[0]:<8} | {row[1]:<9} | '{preview}'")
                    print("-------------------------------\n")
                
                # --- 5. Adaptive Network Metrics ---
                elif user_input.lower() == '/metrics':
                    print("\n--- Adaptive Network Metrics ---")
                    current_time = time.time()
                    
                    if not self.peer_health:
                        print("No active peers to measure.")
                        
                    for peer, health in self.peer_health.items():
                        idle_time = current_time - health['last_seen']
                        status = "Active (Chatting)" if idle_time < 5.0 else "Idle (Inactive)"
                        
                        fixed_bandwidth_bps = 60 / 10.0
                        if idle_time < health['interval']:
                            adaptive_bps = 0.0
                        else:
                            adaptive_bps = 60 / health['interval']
                            
                        detection_remaining = max(0.0, self.peer_timeout - idle_time)

                        print(f"Peer: {peer} [{status}]")
                        print(f"  RTT: {health['rtt']:.4f}s | Current Interval: {health['interval']}s")
                        print(f"  Idle Time: {idle_time:.1f}s | Drop in: {detection_remaining:.1f}s")
                        print(f"  Bandwidth: {adaptive_bps:.1f} B/s (vs Fixed {fixed_bandwidth_bps:.1f} B/s)")
                        print(f"  Efficiency Gain: {max(0, 100 - (adaptive_bps/fixed_bandwidth_bps * 100)):.0f}%\n")
                    print("--------------------------------\n")

                # --- 6. Send File Command ---
                elif user_input.startswith('/sendfile '):
                    parts = user_input.split(' ', 2)
                    if len(parts) == 3:
                        target = parts[1]
                        filepath = parts[2]
                        self.request_file_transfer(target, filepath)
                    else:
                        print("[Error] Format: /sendfile username C:\\path\\to\\file.txt")

                # --- 7. Pause File Transfer ---
                elif user_input.startswith('/pause '):
                    parts = user_input.split(' ')
                    if len(parts) == 2:
                        transfer_id = parts[1]
                        self.db.update_file_progress(transfer_id, status="PAUSED")
                        print(f"[System] Pause signal sent for transfer {transfer_id}.")
                    else:
                        print("[Error] Format: /pause transfer_id")

                # --- 8. Resume File Transfer ---
                elif user_input.startswith('/resume '):
                    parts = user_input.split(' ')
                    if len(parts) == 2:
                        transfer_id = parts[1]
                        with self.db.get_connection() as conn:
                            cursor = conn.cursor()
                            cursor.execute("SELECT peer_username FROM file_transfers WHERE transfer_id=?", (transfer_id,))
                            result = cursor.fetchone()
                            
                        if result:
                            target = result[0]
                            self.db.update_file_progress(transfer_id, status="IN_PROGRESS")
                            print(f"\n[System] Resuming transfer {transfer_id}...\n> ", end="")
                            threading.Thread(target=self.transmit_file_worker, args=(target, transfer_id), daemon=True).start()
                        else:
                            print(f"[Error] Transfer ID {transfer_id} not found.")
                    else:
                        print("[Error] Format: /resume transfer_id")

                # --- NEW: View Active File Transfers ---
                elif user_input.lower() == '/transfers':
                    print("\n--- Active File Transfers ---")
                    with self.db.get_connection() as conn:
                        cursor = conn.cursor()
                        # Fetch all transfers that aren't finished yet
                        cursor.execute("SELECT transfer_id, file_name, chunks_completed, total_chunks, status, direction FROM file_transfers WHERE status != 'COMPLETED'")
                        rows = cursor.fetchall()
                        if not rows:
                            print(" No active or paused transfers.")
                        for row in rows:
                            tid, fname, completed, total, status, direction = row
                            progress = (completed / total) * 100 if total > 0 else 0
                            # Print a clean, dashboard-style readout
                            print(f" [{direction}] {fname} | {progress:.1f}% | Status: {status}")
                            print(f"    -> ID: {tid}")
                    print("-----------------------------\n")

                else:
                    print("[System] Unknown command. Type /peers, /info, /status, /sendfile, or @username message")

            except KeyboardInterrupt:
                self.shutdown()

    def shutdown(self):
        self.running = False
        self.host_socket.close()
        self.p2p_socket.close()
        sys.exit(0)

    def initiate_sync(self, target_username):
        """Requests the peer's current sequence state to determine what is missing."""
        
        # 1. Prevent Duplicate Syncs
        with self.sync_lock:
            if target_username in self.active_syncs:
                return  # We are already syncing with this user, abort duplicate.
            self.active_syncs.add(target_username)
            
        try:
            target_info = self.roster.get(target_username)
            if not target_info: 
                return
                
            sync_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sync_sock.settimeout(2.0)
            sync_sock.connect((target_info["ip"], int(target_info["port"])))
            
            # Send Sync Request
            protocol.send_message(sync_sock, protocol.TYPE_SYNC_REQ, {"sender": self.username})
            
            # Wait for Sync Response
            resp_type, resp_payload = protocol.receive_message(sync_sock)
            if resp_type == protocol.TYPE_SYNC_RESP:
                peer_inbound_seq = resp_payload.get("last_inbound_seq", 0)
                print(f"\n[System] Synchronizing with {target_username} (Peer is at Sequence {peer_inbound_seq})...\n> ", end="")
                
                # 2. Process Queue (This uses _send_with_retry, which automatically handles ACKs & DB updates!)
                self.process_sync_queue(target_username, peer_inbound_seq)
            else:
                print(f"\n[Warning] Sync with {target_username} failed: Unexpected response '{resp_type}'.\n> ", end="")
                
            sync_sock.close()
            
        except socket.timeout:
            print(f"\n[Warning] Sync with {target_username} timed out. Will retry on next roster update.\n> ", end="")
        except Exception as e:
            print(f"\n[Warning] Sync with {target_username} failed due to network error: {e}\n> ", end="")
            
        finally:
            # 3. Always release the lock, even if the connection crashed
            with self.sync_lock:
                self.active_syncs.discard(target_username)

    def process_sync_queue(self, target_username, peer_inbound_seq):
        """Flushes the outbox based on the peer's actual state."""
        pending_messages = self.db.get_outbox_messages(target_username)
        if not pending_messages:
            return
            
        for msg_id, payload_json in pending_messages:
            chat_payload = json.loads(payload_json)
            msg_seq = chat_payload.get("seq_num", 0)

            if msg_seq <= peer_inbound_seq:
                # Peer already received this message (likely crashed before sending ACK).
                # Clean up our database to match.
                self.db.remove_from_outbox(msg_id)
                self.db.update_message_status(msg_id, "DELIVERED")
            else:
                # Peer is missing this message. Dispatch to retry worker.
                threading.Thread(
                    target=self._send_with_retry, 
                    args=(target_username, chat_payload), 
                    daemon=True
                ).start()

    def heartbeat_loop(self):
        """Monitors peer health using dynamically adjusting intervals."""
        while self.running:
            current_time = time.time()
            
            for peer in list(self.roster.keys()):
                if peer == self.username or peer not in self.peer_health:
                    continue
                    
                health = self.peer_health[peer]
                
                # 1. Check for Peer Timeout
                if current_time - health["last_seen"] > self.peer_timeout:
                    print(f"\n[Warning] {peer} timed out! Marking offline and starting auto-reconnect.\n> ", end="")
                    
                    self.reconnecting_peers[peer] = {
                        "ip": self.roster[peer]["ip"],
                        "port": self.roster[peer]["port"],
                        "attempts": 0,
                        "next_try": current_time + 2.0
                    }
                    
                    del self.roster[peer] 
                    del self.peer_health[peer]
                    continue
                    
                # 2. Check if it's time for their specific dynamic heartbeat
                if current_time - health["last_seen"] >= health["interval"]:
                    threading.Thread(target=self.send_ping, args=(peer,), daemon=True).start()
                
            # Run sweep every 1 second (instead of 10) to honor precise custom intervals
            time.sleep(1.0)  

    def send_ping(self, target_username):
        """Sends a ping, measures Round Trip Time (RTT), and adjusts intervals."""
        target_info = self.roster.get(target_username)
        if not target_info: 
            return
            
        ping_start_time = time.time()
        
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(2.0)
            sock.connect((target_info["ip"], int(target_info["port"])))
            
            protocol.send_message(sock, protocol.TYPE_PING, {"sender": self.username})
            resp_type, _ = protocol.receive_message(sock)
            
            if resp_type == protocol.TYPE_PONG:
                rtt = time.time() - ping_start_time
                
                if target_username in self.peer_health:
                    self.peer_health[target_username]["last_seen"] = time.time()
                    self.peer_health[target_username]["rtt"] = rtt
                    
                    # Adaptive Logic:
                    # Fast connection (< 100ms) -> Ping less often (15s) to save bandwidth
                    # Slow connection (>= 100ms) -> Ping more often (5s) to monitor health closely
                    new_interval = 15.0 if rtt < 0.1 else 5.0
                    self.peer_health[target_username]["interval"] = new_interval
                    
            sock.close()
        except Exception:
            pass

    def auto_reconnect_loop(self):
        """Attempts to reconnect to dropped peers using exponential backoff."""
        while self.running:
            current_time = time.time()
            
            for peer, info in list(self.reconnecting_peers.items()):
                if current_time >= info["next_try"]:
                    success = False
                    try:
                        # Attempt Ping
                        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        sock.settimeout(2.0)
                        sock.connect((info["ip"], int(info["port"])))
                        
                        protocol.send_message(sock, protocol.TYPE_PING, {"sender": self.username})
                        resp_type, _ = protocol.receive_message(sock)
                        sock.close()
                        
                        if resp_type == protocol.TYPE_PONG:
                            success = True
                            print(f"\n[System] Reconnection to {peer} successful! Restoring state...\n> ", end="")
                            
                            self.roster[peer] = {"ip": info["ip"], "port": info["port"]}
                            self.peer_health[peer] = {
                                "last_seen": current_time,
                                "rtt": 0.0,
                                "interval": 10.0
                            }
                            del self.reconnecting_peers[peer]
                            
                            threading.Thread(target=self.initiate_sync, args=(peer,), daemon=True).start()
                    except Exception:
                        pass # Ignore the crash, handle it in the success check below
                        
                    # --- NEW: Catch ALL failures securely ---
                    if not success:
                        info["attempts"] += 1
                        backoff_delay = min(60.0, 2 ** info["attempts"]) 
                        info["next_try"] = time.time() + backoff_delay
            
            time.sleep(1.0)

    def get_file_hash(self, filepath):
        """Generates a SHA-256 hash to guarantee file integrity."""
        sha256 = hashlib.sha256()
        with open(filepath, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256.update(byte_block)
        return sha256.hexdigest()

    def request_file_transfer(self, target_username, filepath):
        """Prepares metadata and sends the transfer request."""
        if target_username not in self.roster:
            print(f"[Error] {target_username} is offline. Files require an active connection.")
            return

        if not os.path.exists(filepath):
            print("[Error] File not found.")
            return

        file_size = os.path.getsize(filepath)
        file_name = os.path.basename(filepath)
        file_hash = self.get_file_hash(filepath)
        transfer_id = uuid.uuid4().hex
        
        chunk_size = 32768 # 32 KB chunks
        total_chunks = (file_size + chunk_size - 1) // chunk_size

        self.db.init_file_transfer(
            transfer_id, target_username, "OUTGOING", file_name, 
            file_size, total_chunks, filepath, file_hash, "REQUESTING"
        )

        payload = {
            "transfer_id": transfer_id,
            "sender": self.username,
            "file_name": file_name,
            "file_size": file_size,
            "total_chunks": total_chunks,
            "file_hash": file_hash
        }
        
        target_info = self.roster[target_username]
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5.0)
            sock.connect((target_info["ip"], int(target_info["port"])))
            protocol.send_message(sock, protocol.TYPE_FILE_REQ, payload)
            
            # --- THE FIX: Wait for Bob's answer on the same socket! ---
            resp_type, resp_payload = protocol.receive_message(sock)
            sock.close()

            # If accepted, launch the chunk transmission thread immediately
            if resp_type == protocol.TYPE_FILE_RESP and resp_payload.get("status") == "ACCEPTED":
                print(f"\n[System] File request accepted! Starting transmission...\n> ", end="")
                self.db.update_file_progress(transfer_id, status="IN_PROGRESS")
                threading.Thread(target=self.transmit_file_worker, args=(target_username, transfer_id), daemon=True).start()
            else:
                self.db.update_file_progress(transfer_id, status="REJECTED")
                print(f"\n[Error] File transfer was rejected or timed out.\n> ", end="")

        except Exception:
            self.db.update_file_progress(transfer_id, status="FAILED")
            print(f"\n[Error] Failed to reach {target_username} for file transfer.\n> ", end="")

    def transmit_file_worker(self, target_username, transfer_id):
        """Streams the file chunks, supporting Seek() and Pause/Resume."""
        target_info = self.roster.get(target_username)
        if not target_info:
            return

        state = self.db.get_transfer_state(transfer_id)
        if not state: return
        
        start_chunk, status, filepath, _, total_chunks = state
        chunk_size = 1048576 * 2
        file_name = os.path.basename(filepath)

        try:
            with open(filepath, 'rb') as f:
                # Exact byte offset calculation for resuming
                byte_offset = start_chunk * chunk_size
                f.seek(byte_offset)
                
                for chunk_idx in range(start_chunk, total_chunks):
                    # Check DB to see if user paused the transfer
                    current_status = self.db.get_transfer_state(transfer_id)[1]
                    if current_status == "PAUSED":
                        print(f"\n[System] Transfer paused at {(chunk_idx/total_chunks)*100:.1f}%\n> ", end="")
                        return
                        
                    raw_bytes = f.read(chunk_size)
                    encoded_data = base64.b64encode(raw_bytes).decode('utf-8')
                    
                    payload = {
                        "transfer_id": transfer_id,
                        "chunk_index": chunk_idx,
                        "data": encoded_data
                    }
                    
                    # --- FIX: Open and close socket PER CHUNK ---
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.settimeout(5.0)
                    sock.connect((target_info["ip"], int(target_info["port"])))
                    
                    protocol.send_message(sock, protocol.TYPE_FILE_CHUNK, payload)
                    ack_type, ack_payload = protocol.receive_message(sock)
                    sock.close()
                    
                    if ack_type == protocol.TYPE_FILE_ACK:
                        self.db.update_file_progress(transfer_id, chunk_index=chunk_idx)
                    else:
                        raise ConnectionError("Invalid ACK received")
                        
                # Transmission Complete
                self.db.update_file_progress(transfer_id, status="COMPLETED")
                print(f"\n[System] File {filepath} successfully sent to {target_username}! (100%)\n> ", end="")
            
        except Exception as e:
            # Drop connection and save exact progress for auto-resume
            self.db.update_file_progress(transfer_id, status="INTERRUPTED")
            print(f"\n[Warning] Transfer interrupted. Progress saved for recovery.\n> ", end="")
            
if __name__ == "__main__":
    host_ip = input("Enter Host IP (e.g. 127.0.0.1 for local testing): ").strip()
    email = input("Email: ").strip()
    passkey = input("Passkey: ").strip()
    
    app = LANClientApp(host_ip)
    if app.login(email, passkey):
        app.run()
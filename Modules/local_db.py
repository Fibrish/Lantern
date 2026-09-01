import sqlite3
import json
import threading

class LocalDB:
    def __init__(self, username):
        self.db_file = f"client_{username}.db"
        self.username = username
        self.db_lock = threading.Lock()
        self.init_db()

    def get_connection(self):
        return sqlite3.connect(self.db_file, check_same_thread=False)

    def init_db(self):
        with self.db_lock, self.get_connection() as conn:
            # --- CRASH SAFETY: Enable Write-Ahead Logging ---
            conn.execute('PRAGMA journal_mode=WAL;')
            cursor = conn.cursor()
            
            # --- UPDATED PEER SCHEMA: Added inbound_seq and outbound_seq ---
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS peers (
                    username TEXT PRIMARY KEY,
                    last_ip TEXT,
                    last_port INTEGER,
                    inbound_seq INTEGER DEFAULT 0,
                    outbound_seq INTEGER DEFAULT 0,
                    last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS messages (
                    msg_id TEXT PRIMARY KEY,
                    seq_num INTEGER,
                    sender TEXT NOT NULL,
                    receiver TEXT NOT NULL,
                    content TEXT,
                    timestamp TEXT,
                    status TEXT DEFAULT 'DELIVERED', 
                    direction TEXT 
                )
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS outbox (
                    msg_id TEXT PRIMARY KEY,
                    target_username TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    attempts INTEGER DEFAULT 0
                )
            ''')
            conn.commit()

    # ==========================================
    # PEER & SYNCHRONIZATION STATE
    # ==========================================
    def update_peer_cache(self, username, ip, port):
        """Updates IP/Port when roster changes, without overwriting sequence numbers."""
        with self.db_lock, self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO peers (username, last_ip, last_port) 
                VALUES (?, ?, ?)
                ON CONFLICT(username) DO UPDATE SET 
                last_ip=excluded.last_ip, last_port=excluded.last_port, last_seen=CURRENT_TIMESTAMP
            ''', (username, ip, port))
            conn.commit()

    def get_seq_numbers(self, username):
        """Retrieves synchronization state for a peer."""
        with self.db_lock, self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT inbound_seq, outbound_seq FROM peers WHERE username=?', (username,))
            result = cursor.fetchone()
            return result if result else (0, 0)

    def update_seq_numbers(self, username, inbound_seq, outbound_seq):
        with self.db_lock, self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE peers SET inbound_seq=?, outbound_seq=? WHERE username=?
            ''', (inbound_seq, outbound_seq, username))
            conn.commit()

    # ==========================================
    # MESSAGE & OUTBOX STATE
    # ==========================================
    def save_message(self, msg_id, seq_num, sender, receiver, content, timestamp, status, direction):
        with self.db_lock, self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR IGNORE INTO messages 
                (msg_id, seq_num, sender, receiver, content, timestamp, status, direction)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (msg_id, seq_num, sender, receiver, content, timestamp, status, direction))
            conn.commit()

    def update_message_status(self, msg_id, status):
        with self.db_lock, self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('UPDATE messages SET status=? WHERE msg_id=?', (status, msg_id))
            conn.commit()

    def add_to_outbox(self, msg_id, target_username, payload_dict):
        payload_json = json.dumps(payload_dict)
        with self.db_lock, self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('INSERT OR REPLACE INTO outbox (msg_id, target_username, payload) VALUES (?, ?, ?)',
                           (msg_id, target_username, payload_json))
            conn.commit()

    def remove_from_outbox(self, msg_id):
        with self.db_lock, self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('DELETE FROM outbox WHERE msg_id=?', (msg_id,))
            conn.commit()

    # ==========================================
    # ATOMIC TRANSACTIONS (CRASH-SAFE WRITES)
    # ==========================================
    def save_outgoing_message(self, target_username, msg_id, content, timestamp, base_payload):
        """Atomically increments sequence, saves message, and queues to outbox."""
        with self.db_lock, self.get_connection() as conn:
            cursor = conn.cursor()
            
            # 1. Fetch and increment outbound sequence
            cursor.execute('SELECT inbound_seq, outbound_seq FROM peers WHERE username=?', (target_username,))
            result = cursor.fetchone()
            inbound_seq, outbound_seq = result if result else (0, 0)
            new_outbound_seq = outbound_seq + 1
            
            # 2. Update sequence in peer table
            cursor.execute('''
                INSERT INTO peers (username, inbound_seq, outbound_seq) 
                VALUES (?, ?, ?)
                ON CONFLICT(username) DO UPDATE SET outbound_seq=?
            ''', (target_username, inbound_seq, new_outbound_seq, new_outbound_seq))
            
            # 3. Save to Messages table
            cursor.execute('''
                INSERT INTO messages (msg_id, seq_num, sender, receiver, content, timestamp, status, direction)
                VALUES (?, ?, ?, ?, ?, ?, 'PENDING', 'OUTGOING')
            ''', (msg_id, new_outbound_seq, self.username, target_username, content, timestamp))
            
            # 4. Finalize payload and save to Outbox
            base_payload["seq_num"] = new_outbound_seq
            payload_json = json.dumps(base_payload)
            cursor.execute('INSERT INTO outbox (msg_id, target_username, payload) VALUES (?, ?, ?)',
                           (msg_id, target_username, payload_json))
                           
            # The 'with conn' block auto-commits here. If any step above fails, EVERYTHING rolls back.
            return new_outbound_seq

    def save_incoming_message(self, sender, msg_id, seq_num, content, timestamp):
        """Atomically verifies sequence and saves incoming message."""
        with self.db_lock, self.get_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute('SELECT inbound_seq, outbound_seq FROM peers WHERE username=?', (sender,))
            result = cursor.fetchone()
            inbound_seq, outbound_seq = result if result else (0, 0)
            
            expected_seq = inbound_seq + 1
            
            # Only process if it perfectly matches the expected sequence
            if seq_num == expected_seq:
                cursor.execute('''
                    INSERT INTO peers (username, inbound_seq, outbound_seq) 
                    VALUES (?, ?, ?)
                    ON CONFLICT(username) DO UPDATE SET inbound_seq=?
                ''', (sender, expected_seq, outbound_seq, expected_seq))
                
                cursor.execute('''
                    INSERT INTO messages (msg_id, seq_num, sender, receiver, content, timestamp, status, direction)
                    VALUES (?, ?, ?, ?, ?, ?, 'DELIVERED', 'INCOMING')
                ''', (msg_id, seq_num, sender, self.username, content, timestamp))
                
                return True 
            
            return False
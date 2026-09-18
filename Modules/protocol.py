import json
import struct

# --- Packet Types ---
TYPE_LOGIN = 1           # Client -> Host: "Here are my credentials and listening port"
TYPE_LOGIN_RESP = 2      # Host -> Client: "Success/Fail + Your Username"
TYPE_ROSTER_UPDATE = 3   # Host -> Client: "Here is the updated list of online users"
TYPE_CHAT = 4            # Client -> Client: P2P Chat message
TYPE_ACK = 5             # <-- NEW: Client -> Client: "I received your message"
TYPE_SYNC_REQ = 10
TYPE_SYNC_RESP = 11
TYPE_PING = 12
TYPE_PONG = 13

# --- File Transfer Packets ---
TYPE_FILE_REQ = 20      # Sender -> Receiver: "Can I send you a file? (Metadata)"
TYPE_FILE_RESP = 21     # Receiver -> Sender: "Accept / Reject"
TYPE_FILE_CHUNK = 22    # Sender -> Receiver: Raw file data chunk
TYPE_FILE_ACK = 23      # Receiver -> Sender: "Chunk received"

def send_message(sock, packet_type, payload):
    """Serializes and sends a packet with a strict 4-byte header."""
    payload_bytes = json.dumps(payload).encode('utf-8')
    header = struct.pack('!BI', packet_type, len(payload_bytes))
    sock.sendall(header + payload_bytes)

def receive_message(sock):
    """Receives and deserializes a packet based on the header."""
    try:
        header = sock.recv(5)
        if not header or len(header) < 5:
            return None, None
            
        packet_type, payload_length = struct.unpack('!BI', header)
        
        payload_bytes = b""
        while len(payload_bytes) < payload_length:
            chunk = sock.recv(min(4096, payload_length - len(payload_bytes)))
            if not chunk:
                return None, None
            payload_bytes += chunk
            
        payload = json.loads(payload_bytes.decode('utf-8'))
        return packet_type, payload
    except Exception:
        return None, None
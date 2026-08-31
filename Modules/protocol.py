import struct
import json

# --- Protocol Details ---
PROTOCOL_VERSION = 1  # Represents version 0.0.1
MAX_PAYLOAD_SIZE = 10 * 1024 * 1024  # 10 MB limit to prevent memory overload

# --- Packet Types ---
TYPE_LOGIN = 1
TYPE_CHAT = 2
TYPE_AUTH_INIT = 5       # Client -> Server: "I want to connect"
TYPE_AUTH_CHALLENGE = 6  # Server -> Client: "Sign this random nonce"
TYPE_AUTH_RESPONSE = 7   # Client -> Server: "Here is my signature and public key"
TYPE_AUTH_ERROR = 8      # Server -> Client: "Authentication failed"
TYPE_AUTH_SUCCESS = 9    # Server -> Client: "Authentication passed, ready for data"

# Header: 1 Byte (Version) + 1 Byte (Type) + 4 Bytes (Payload Size) = 6 Bytes total
HEADER_FORMAT = '!BBI'
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

def send_message(socket_obj, packet_type, payload_dict):
    """Serializes and sends a packet, catching encoding errors."""
    try:
        serialized_payload = json.dumps(payload_dict).encode('utf-8')
        payload_length = len(serialized_payload)
        
        # Pack the protocol version into the header alongside the type and length
        header = struct.pack(HEADER_FORMAT, PROTOCOL_VERSION, packet_type, payload_length)
        socket_obj.sendall(header + serialized_payload)
        return True
    except (TypeError, ValueError) as e:
        print(f"[Protocol Error] Failed to serialize payload: {e}")
        return False
    except Exception as e:
        print(f"[Network Error] Failed to send packet: {e}")
        return False

def receive_message(socket_obj):
    """Reads a packet with strict validation to drop malformed data."""
    try:
        # 1. Read and Unpack Header
        header_bytes = _recv_all(socket_obj, HEADER_SIZE)
        if not header_bytes:
            return None, None
            
        version, packet_type, payload_length = struct.unpack(HEADER_FORMAT, header_bytes)
        
        # 2. Version Verification
        if version != PROTOCOL_VERSION:
            print(f"[Protocol Error] Version mismatch. Expected {PROTOCOL_VERSION}, got {version}. Dropping packet.")
            return None, None
            
        # 3. Payload Size Validation (Prevents malicious/corrupted huge sizes)
        if payload_length > MAX_PAYLOAD_SIZE:
            print(f"[Protocol Error] Payload exceeds max size ({payload_length} bytes). Dropping packet.")
            return None, None
            
        # 4. Read and Decode Payload
        payload_bytes = _recv_all(socket_obj, payload_length)
        if not payload_bytes:
            return None, None
            
        deserialized_payload = json.loads(payload_bytes.decode('utf-8'))
        return packet_type, deserialized_payload
        
    except struct.error:
        print("[Protocol Error] Malformed binary header received.")
        return None, None
    except json.JSONDecodeError:
        print("[Protocol Error] Malformed JSON payload received.")
        return None, None
    except UnicodeDecodeError:
        print("[Protocol Error] Failed to decode payload as UTF-8.")
        return None, None
    except Exception as e:
        print(f"[Unknown Error] {e}")
        return None, None

def _recv_all(socket_obj, target_bytes):
    """Safely retrieves exact byte counts."""
    buffer = b''
    while len(buffer) < target_bytes:
        try:
            chunk = socket_obj.recv(target_bytes - len(buffer))
            if not chunk:
                return None 
            buffer += chunk
        except Exception:
            return None
    return buffer
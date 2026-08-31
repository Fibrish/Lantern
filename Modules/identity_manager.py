import os
import hashlib
import base64
from cryptography.hazmat.primitives.asymmetric import rsa, padding as rsa_padding
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
import json

class IdentityManager:
    def __init__(self, storage_dir=".identity"):
        self.storage_dir = storage_dir
        self.private_key_file = os.path.join(storage_dir, "private_key.pem")
        self.public_key_file = os.path.join(storage_dir, "public_key.pem")
        
        self.private_key = None
        self.public_key = None
        self.node_id = None
        
        # Automatically load existing keys or create new ones on startup
        self._initialize_identity()

    def _initialize_identity(self):
        """Loads the key pair if it exists, otherwise generates a new one."""
        if not os.path.exists(self.storage_dir):
            os.makedirs(self.storage_dir)

        if os.path.exists(self.private_key_file) and os.path.exists(self.public_key_file):
            self._load_keys()
            print(f"[Identity] Loaded existing identity. Node ID: {self.node_id[:8]}...")
        else:
            print("[Identity] Generating new cryptographic identity...")
            self._generate_and_save_keys()
            print(f"[Identity] New identity created. Node ID: {self.node_id[:8]}...")

    def _generate_and_save_keys(self):
        """Generates a secure RSA key pair and saves them to local files."""
        # 1. Generate Private Key
        self.private_key = rsa.generate_private_key(
            public_exponent=65537,
            key_size=2048,
        )
        
        # 2. Extract Public Key
        self.public_key = self.private_key.public_key()
        
        # 3. Save Private Key (Unencrypted for this offline LAN scope)
        with open(self.private_key_file, "wb") as f:
            f.write(self.private_key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.TraditionalOpenSSL,
                encryption_algorithm=serialization.NoEncryption()
            ))
            
        # 4. Save Public Key
        with open(self.public_key_file, "wb") as f:
            f.write(self.public_key.public_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PublicFormat.SubjectPublicKeyInfo
            ))
            
        self._generate_node_id()

    def _load_keys(self):
        """Reads existing keys from the local file system."""
        with open(self.private_key_file, "rb") as f:
            self.private_key = serialization.load_pem_private_key(
                f.read(),
                password=None,
            )
            
        with open(self.public_key_file, "rb") as f:
            self.public_key = serialization.load_pem_public_key(
                f.read()
            )
            
        self._generate_node_id()

    def _generate_node_id(self):
        """Creates a unique fingerprint (Node ID) by hashing the public key."""
        public_bytes = self.public_key.public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo
        )
        # Hash the bytes using SHA-256 and convert to a hex string
        self.node_id = hashlib.sha256(public_bytes).hexdigest()

    def get_public_key_bytes(self):
        """Returns the public key as raw bytes to share over the network."""
        return self.public_key.public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo
        )

    def sign_challenge(self, challenge_text):
        """Signs a random string using this node's Private Key."""
        signature = self.private_key.sign(
            challenge_text.encode('utf-8'),
            rsa_padding.PSS(
                mgf=rsa_padding.MGF1(hashes.SHA256()),
                salt_length=rsa_padding.PSS.MAX_LENGTH
            ),
            hashes.SHA256()
        )
        # Encode as Base64 to safely send as a JSON string
        return base64.b64encode(signature).decode('utf-8')

    @staticmethod
    def verify_signature(pub_key_pem, challenge_text, signature_b64):
        """Uses a peer's Public Key to mathematically verify their signature."""
        try:
            pub_key = serialization.load_pem_public_key(pub_key_pem.encode('utf-8'))
            signature = base64.b64decode(signature_b64.encode('utf-8'))
            
            pub_key.verify(
                signature,
                challenge_text.encode('utf-8'),
                rsa_padding.PSS(
                    mgf=rsa_padding.MGF1(hashes.SHA256()),
                    salt_length=rsa_padding.PSS.MAX_LENGTH
                ),
                hashes.SHA256()
            )
            return True
        except Exception:
            return False

    @staticmethod
    def verify_node_id(pub_key_pem, claimed_node_id):
        """Ensures a Public Key mathematically matches the claimed Node ID."""
        pub_key = serialization.load_pem_public_key(pub_key_pem.encode('utf-8'))
        pub_bytes = pub_key.public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo
        )
        calculated_id = hashlib.sha256(pub_bytes).hexdigest()
        return calculated_id == claimed_node_id

    @staticmethod
    def generate_session_key():
        """Generates a random one-time 256-bit AES key."""
        return AESGCM.generate_key(bit_length=256)

    def encrypt_session_key(self, peer_pub_key_pem, session_key_bytes):
        """Encrypts the AES session key using the peer's RSA Public Key."""
        pub_key = serialization.load_pem_public_key(peer_pub_key_pem.encode('utf-8'))
        encrypted_key = pub_key.encrypt(
            session_key_bytes,
            rsa_padding.OAEP(
                mgf=rsa_padding.MGF1(algorithm=hashes.SHA256()),
                algorithm=hashes.SHA256(),
                label=None
            )
        )
        return base64.b64encode(encrypted_key).decode('utf-8')

    def decrypt_session_key(self, encrypted_key_b64):
        """Decrypts the AES session key using our own RSA Private Key."""
        encrypted_key_bytes = base64.b64decode(encrypted_key_b64.encode('utf-8'))
        session_key = self.private_key.decrypt(
            encrypted_key_bytes,
            rsa_padding.OAEP(
                mgf=rsa_padding.MGF1(algorithm=hashes.SHA256()),
                algorithm=hashes.SHA256(),
                label=None
            )
        )
        return session_key

    # ==========================================
    # MESSAGE ENCRYPTION (AES-GCM)
    # ==========================================
    @staticmethod
    def encrypt_payload(session_key, payload_dict):
        """Encrypts and authenticates a JSON payload using AES-GCM."""
        aesgcm = AESGCM(session_key)
        nonce = os.urandom(12) # AES-GCM requires a unique 12-byte nonce per message
        
        payload_bytes = json.dumps(payload_dict).encode('utf-8')
        ciphertext = aesgcm.encrypt(nonce, payload_bytes, associated_data=None)
        
        # Prepend the nonce to the ciphertext so the receiver can use it to decrypt
        combined_data = nonce + ciphertext
        return base64.b64encode(combined_data).decode('utf-8')

    @staticmethod
    def decrypt_payload(session_key, encrypted_b64):
        """Decrypts and verifies the payload. Fails if data was tampered with."""
        combined_data = base64.b64decode(encrypted_b64.encode('utf-8'))
        nonce = combined_data[:12]
        ciphertext = combined_data[12:]
        
        aesgcm = AESGCM(session_key)
        # This will throw cryptography.exceptions.InvalidTag if tampered with
        decrypted_bytes = aesgcm.decrypt(nonce, ciphertext, associated_data=None)
        return json.loads(decrypted_bytes.decode('utf-8'))
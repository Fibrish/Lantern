import socket
import threading
import queue

class NetworkManager:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.message_queue = queue.Queue()
        self.is_connected = False

    def connect(self):
        """Establishes connection and starts background listening."""
        try:
            self.client_socket.connect((self.host, self.port))
            self.is_connected = True
            
            # Start internal listener thread
            threading.Thread(target=self._listen_for_data, daemon=True).start()
            return True
        except ConnectionRefusedError:
            return False

    def _listen_for_data(self):
        """Internal method running on a thread to populate the queue."""
        while self.is_connected:
            try:
                message = self.client_socket.recv(1024).decode('utf-8')
                if not message:
                    self.disconnect()
                    break
                self.message_queue.put(message)
            except Exception:
                self.disconnect()
                break

    def send(self, text):
        """Public method for the app to send data."""
        if self.is_connected:
            self.client_socket.send(text.encode('utf-8'))

    def get_message(self):
        """Public method for the app to safely retrieve data without blocking."""
        if not self.message_queue.empty():
            return self.message_queue.get()
        return None

    def disconnect(self):
        """Safely shuts down the connection."""
        self.is_connected = False
        self.client_socket.close()
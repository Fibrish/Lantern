import Modules.protocol as protocol
import socket
import threading
import queue
import time

class NetworkManager:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.message_queue = queue.Queue()
        self.is_connected = False

    def connect(self):
        try:
            self.client_socket.connect((self.host, self.port))
            self.is_connected = True
            threading.Thread(target=self._listen_for_data, daemon=True).start()
            return True
        except ConnectionRefusedError:
            return False

    def _listen_for_data(self):
        """Continuously reads framed messages and queues the parsed dictionaries."""
        while self.is_connected:
            packet_type, payload = protocol.receive_message(self.client_socket)
            
            if packet_type is None:
                self.disconnect()
                break
                
            # Place both the type and the dictionary into the queue
            self.message_queue.put((packet_type, payload))

    def send_chat(self, text_content):
        """Packages text into a dictionary and sends it via the protocol."""
        if self.is_connected:
            payload = {"content": text_content}
            protocol.send_message(self.client_socket, protocol.TYPE_CHAT, payload)

    def get_message(self):
        if not self.message_queue.empty():
            return self.message_queue.get()
        return None

    def disconnect(self):
        self.is_connected = False
        self.client_socket.close()

# --- Application UI Simulation ---
def ui_event_loop(network):
    while network.is_connected:
        msg_data = network.get_message()
        if msg_data:
            packet_type, payload = msg_data
            
            if packet_type == protocol.TYPE_CHAT:
                sender = payload.get("sender", "Unknown")
                content = payload.get("content", "")
                print(f"\n[{sender}]: {content}\nType your message: ", end="")
                
        time.sleep(0.1)

if __name__ == "__main__":
    net = NetworkManager('127.0.0.1', 5000)
    
    if net.connect():
        print("Connected to Server! (Type 'exit' to quit)\n")
        threading.Thread(target=ui_event_loop, args=(net,), daemon=True).start()
        
        while True:
            user_input = input("Type your message: ")
            if user_input.lower() == 'exit':
                break
            if user_input:
                net.send_chat(user_input)
                
        net.disconnect()
    else:
        print("Failed to connect to the server.")
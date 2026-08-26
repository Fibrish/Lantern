import Modules.protocol as protocol
import socket
import threading

active_clients = []

def broadcast(packet_type, payload_dict, sender_socket=None):
    """Safely broadcasts a structured packet to all clients except the sender."""
    for client in active_clients:
        if client != sender_socket:
            # If send_message returns False, the client dropped
            success = protocol.send_message(client, packet_type, payload_dict)
            if not success:
                remove_client(client)

def remove_client(client_socket):
    if client_socket in active_clients:
        active_clients.remove(client_socket)
        client_socket.close()

def handle_client(client_socket, client_address):
    print(f"\n[JOINED] {client_address} connected.")
    active_clients.append(client_socket)
    
    # Broadcast a system message to others
    join_payload = {"sender": "SERVER", "content": f"User {client_address[1]} joined."}
    broadcast(protocol.TYPE_CHAT, join_payload, client_socket)
    
    while True:
        # Blocks until a complete, valid packet arrives
        packet_type, payload = protocol.receive_message(client_socket)
        
        # If None is returned, the client disconnected or sent invalid data
        if packet_type is None:
            break 
            
        # Route the packet based on its type
        if packet_type == protocol.TYPE_CHAT:
            # Inject the sender's temporary ID before broadcasting
            payload["sender"] = f"User {client_address[1]}"
            print(f"[LOG] {payload['sender']}: {payload.get('content', '')}")
            
            broadcast(protocol.TYPE_CHAT, payload, client_socket)
            
    print(f"\n[LEFT] {client_address} disconnected.")
    remove_client(client_socket)
    
    leave_payload = {"sender": "SERVER", "content": f"User {client_address[1]} left."}
    broadcast(protocol.TYPE_CHAT, leave_payload)

def start_server():
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind(('127.0.0.1', 5000))
    server_socket.listen()
    print("[STARTING] Protocol Server v0.0.1 listening on 127.0.0.1:5000...")

    while True:
        client_socket, client_address = server_socket.accept()
        threading.Thread(
            target=handle_client, 
            args=(client_socket, client_address),
            daemon=True
        ).start()

if __name__ == "__main__":
    start_server()
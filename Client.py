import socket
import threading

# 1. A list to keep track of all connected client sockets
active_clients = []

def broadcast(message, sender_socket):
    """Routes an incoming message to all other connected clients."""
    for client in active_clients:
        # Don't echo the message back to the person who sent it
        if client != sender_socket:
            try:
                client.send(message.encode('utf-8'))
            except Exception:
                # If sending fails, the client disconnected unexpectedly
                remove_client(client)

def remove_client(client_socket):
    """Safely removes a disconnected client from the active list."""
    if client_socket in active_clients:
        active_clients.remove(client_socket)
        client_socket.close()

def handle_client(client_socket, client_address):
    """Handles continuous listening for a single client."""
    print(f"\n[JOINED] {client_address} connected.")
    active_clients.append(client_socket)
    
    # Announce new user to everyone else
    broadcast(f"User {client_address[1]} has joined the chat.", client_socket)
    
    while True:
        try:
            message = client_socket.recv(1024).decode('utf-8')
            if not message:
                break
                
            # 2. When a message is received, broadcast it to the group
            formatted_msg = f"User {client_address[1]}: {message}"
            print(f"[LOG] {formatted_msg}")
            broadcast(formatted_msg, client_socket)
            
        except ConnectionResetError:
            break
            
    print(f"\n[LEFT] {client_address} disconnected.")
    remove_client(client_socket)
    broadcast(f"User {client_address[1]} has left the chat.", client_socket)

def start_server():
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind(('127.0.0.1', 5000))
    server_socket.listen()
    print("[STARTING] Multi-User Server is listening on 127.0.0.1:5000...")

    while True:
        client_socket, client_address = server_socket.accept()
        
        # Spawn thread for the new user
        thread = threading.Thread(
            target=handle_client, 
            args=(client_socket, client_address),
            daemon=True
        )
        thread.start()

if __name__ == "__main__":
    start_server()
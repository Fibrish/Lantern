import socket
import threading
# Multi Threaded Server

def handle_client(client_socket, client_address):
    """Handles all communication for a single connected client."""
    print(f"\n[NEW CONNECTION] {client_address} connected.")
    
    while True:
        try:
            message = client_socket.recv(1024).decode('utf-8')
            
            if not message:
                break 
                
            print(f"[{client_address[1]}] says: {message}")
            client_socket.send(f"Message received by server!".encode('utf-8'))
            
        except ConnectionResetError:
            break
            
    print(f"\n[DISCONNECTED] {client_address} disconnected.")
    client_socket.close()

def start_server():
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind(('127.0.0.1', 5000))
    server_socket.listen()
    print("[STARTING] Server is listening on 127.0.0.1:5000...")

    while True:
        client_socket, client_address = server_socket.accept()
        
        client_thread = threading.Thread(
            target=handle_client, 
            args=(client_socket, client_address)
        )
        client_thread.daemon = True 
        client_thread.start()
        
        print(f"[ACTIVE CONNECTIONS] {threading.active_count() - 1}")

if __name__ == "__main__":
    start_server()
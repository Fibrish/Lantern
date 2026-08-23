import socket
import threading
import queue
import time

def listen_for_messages(sock, msg_queue):
    """Listens for data from the server and safely hands it off to the queue."""
    while True:
        try:
            message = sock.recv(1024).decode('utf-8')
            if not message:
                print("\n[Server closed the connection]")
                break
            msg_queue.put(message)
        except Exception:
            break

def process_queue(msg_queue):
    """Continuously checks the queue and prints messages to the screen."""
    while True:
        if not msg_queue.empty():
            new_message = msg_queue.get()
            # Overwrite the input line to display the incoming message cleanly
            print(f"[Report]: {new_message}\n", end="")
        time.sleep(0.1) # Prevent high CPU usage

def start_client():
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    try:
        client_socket.connect(('127.0.0.1', 5000))
        print("Connected to server! (Type 'exit' to quit)")
    except ConnectionRefusedError:
        print("Server is offline.")
        return

    message_queue = queue.Queue()

    # 1. Thread for receiving data from the network
    listener_thread = threading.Thread(
        target=listen_for_messages, 
        args=(client_socket, message_queue),
        daemon=True
    )
    listener_thread.start()

    # 2. Thread for safely processing the queue
    queue_thread = threading.Thread(
        target=process_queue, 
        args=(message_queue,),
        daemon=True
    )
    queue_thread.start()

    # 3. Main Thread handles user input (Sending)
    while True:
        try:
            user_input = input("\nType your message: ")
            if user_input.lower() == 'exit':
                break
            if user_input:
                client_socket.send(user_input.encode('utf-8'))
        except KeyboardInterrupt:
            break
        time.sleep(0.1)
            
    client_socket.close()

if __name__ == "__main__":
    start_client()
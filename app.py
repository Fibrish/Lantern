from Modules.Network import NetworkManager
import time
import threading

# Simulate a GUI event loop (like Tkinter's .after() method)
def ui_event_loop(network):
    while network.is_connected:
        # The app simply polls the network manager for new data
        new_msg = network.get_message()
        if new_msg:
            print(f"[App report] Received: {new_msg}\n", end="")
        time.sleep(0.1)

if __name__ == "__main__":
    # 1. Initialize the Network component
    net = NetworkManager('127.0.0.1', 5000)
    
    # 2. Attempt to connect
    if net.connect():
        print("Connected to Server! (Type 'exit' to quit)")
        
        # 3. Start our application's UI loop
        threading.Thread(target=ui_event_loop, args=(net,), daemon=True).start()
        
        # 4. Handle Application Input
        while True:
            user_input = input("\nType your message: ")
            if user_input.lower() == 'exit':
                break
            if user_input:
                net.send(user_input) # Simple, clean send command
            time.sleep(0.1)
        net.disconnect()
    else:
        print("Failed to connect to the server.")
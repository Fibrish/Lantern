# Offline LAN Chat Application

A fully decentralized, peer-to-peer chat application designed to run entirely offline on a Local Area Network (LAN). 

## Features
* **Zero-Configuration Discovery:** Automatically finds other users on the network using mDNS.
* **Custom Protocol:** Robust TCP packet framing prevents data corruption.
* **Offline Storage:** Local SQLite databases ensure chat history remains on your personal device. *(In Development)*

## How to Run
1. Create a virtual environment: `python -m venv venv`
2. Activate the environment: 
   - Windows: `venv\Scripts\activate`
   - Mac/Linux: `source venv/bin/activate`
3. Install dependencies: `pip install -r requirements.txt`
4. Run the application: `python app.py`
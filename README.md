# MQTT to Cloud Data Replayer

## Setup

1. **Create virtual environment:**
   ```bash
   python -m venv venv
   ```

2. **Activate virtual environment:**
   ```bash
   # Windows
   venv\Scripts\activate
   
   # Linux/Mac
   source venv/bin/activate
   ```

3. **Install requirements:**
   ```bash
   pip install -r requirements.txt
   ```

## Run the MQTT Replayer

### With authentication:
```bash
python simple_mqtt_replayer.py --csv simple_day.csv --host < url > --port 8883 --username < username > --password < password > --speed 1
```

### Options:
- `--speed 10.0` - Speed up replay (10x faster)
- `--loop` - Loop the data continuously
- `--port 1883` - Use non-TLS port
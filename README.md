# 🌤️ Real-Time Weather Streaming Dashboard

A complete big data streaming pipeline that collects real-time weather data, processes it through Apache Kafka, stores it in MongoDB, and visualizes it using an interactive Streamlit dashboard.

## 📋 Project Overview

This project demonstrates a full streaming data architecture using:

- **Apache Kafka** for real-time message streaming
- **MongoDB** for historical data storage
- **Streamlit** for interactive data visualization
- **Avro** for schema-based serialization
- **WeatherAPI** for live weather data

## 🏗️ Architecture

```
Weather API → Producer → Kafka → Storage Consumer → MongoDB
                                        ↓
                                  Dashboard (Streamlit)
```

## 🚀 Features

- **Real-Time Streaming**: Live weather data from multiple Philippine cities
- **Historical Analysis**: Query and visualize historical weather patterns
- **Data Export**: Download filtered data in CSV, JSON, or Excel formats
- **Interactive Visualizations**: Bar charts, line graphs, distributions, and heatmaps
- **Time-Series Analysis**: Track weather changes over time
- **MongoDB Integration**: Persistent storage for long-term analytics

## 📦 Prerequisites

- Python 3.8+
- Apache Kafka
- MongoDB (Docker recommended)
- Virtual environment (venv)

## 🛠️ Installation

### 1. Clone the Repository

```bash
git clone <your-repository-url>
cd big-data-streamlit-template
```

### 2. Set Up Virtual Environment

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

Required packages:
```
streamlit
kafka-python
pymongo
pandas
plotly
avro-python3
requests
streamlit-autorefresh
openpyxl
```

### 4. Start MongoDB

```bash
sudo docker run -d \
  --name mongodb \
  -p 27017:27017 \
  -v ~/mongodb-data:/data/db \
  mongo:latest
```

Verify MongoDB is running:
```bash
sudo docker ps | grep mongodb
```

### 5. Configure WeatherAPI

1. Get a free API key from [WeatherAPI.com](https://www.weatherapi.com/)
2. Add your API key to `producer.py`:

```python
WEATHER_API_KEY = 'your_api_key_here'
```

## 🎯 Usage

### Starting the Pipeline

You need **4 terminal windows** to run the complete pipeline:

#### Terminal 1: Kafka Server
```bash
cd ~/kafka
./bin/kafka-server-start.sh config/controller.properties
```

#### Terminal 2: Producer (sends weather data)
```bash
source venv/bin/activate
python producer.py
```

#### Terminal 3: Storage Consumer (saves to MongoDB)
```bash
source venv/bin/activate
python storage_consumer.py
```

#### Terminal 4: Dashboard (visualization)
```bash
source venv/bin/activate
streamlit run app.py
```

### Accessing the Dashboard

Open your browser and navigate to:
```
http://localhost:8501
```

## 📊 Dashboard Features

### Real-Time Streaming Tab
- Live weather data from Kafka
- Current weather by location (bar charts)
- Time-series trends
- Latest messages table
- Auto-refresh (configurable interval)

### Historical Data Tab
- Query MongoDB by time range, location, and metric
- Historical trend charts
- Distribution analysis (box plots)
- Time-of-day patterns
- Data export (CSV/JSON/Excel)
- Full data table with all readings

## 🗂️ Project Structure

```
big-data-streamlit-template/
├── producer.py             # Kafka producer (fetches weather data)
├── storage_consumer.py     # Kafka consumer (stores to MongoDB)
├── app.py                  # Streamlit dashboard
├── sensor_schema.avsc      # Avro schema definition
├── requirements.txt        # Python dependencies
├── venv/                   # Virtual environment
└── README.md               # This file
```

## 🔧 Configuration

### Kafka Topic
Topic name: `sensor-data`

To reset/clean Kafka data:
```bash
cd ~/kafka
./bin/kafka-topics.sh --delete --topic sensor-data --bootstrap-server localhost:9092
./bin/kafka-topics.sh --create --topic sensor-data \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1
```

### MongoDB
Database: `weather_dashboard`
Collection: `sensor_readings`

To clean MongoDB data:
```bash
sudo docker exec -it mongodb mongosh --eval "use weather_dashboard; db.sensor_readings.deleteMany({})"
```

## 🌍 Data Sources

Weather data is collected from the following Philippine cities:
- Quezon City
- Manila
- Makati
- Pasig
- Caloocan

Metrics collected:
- **Temperature** (°C)
- **Humidity** (%)
- **Pressure** (hPa)

## 🔍 Troubleshooting

### Kafka won't start
```bash
# Check if port 9092 is in use
sudo lsof -i :9092

# Check Kafka logs
tail -f ~/kafka/logs/server.log
```

### MongoDB connection issues
```bash
# Start MongoDB
sudo docker start mongodb

# Check MongoDB logs
sudo docker logs mongodb
```

### Producer not fetching data
- Verify your WeatherAPI key is valid
- Check internet connection
- Ensure API rate limits aren't exceeded

### Dashboard shows "No data"
- Ensure producer is running and sending data
- Check Kafka topic has messages:

```bash
cd ~/kafka
./bin/kafka-run-class.sh kafka.tools.GetOffsetShell \
  --broker-list localhost:9092 \
  --topic sensor-data
```

## 📈 Performance Notes

- Producer sends data every 10 seconds (configurable)
- Dashboard auto-refreshes every 15 seconds (adjustable in sidebar)
- MongoDB stores unlimited historical data
- Kafka retains data for 7 days (default)

## 👨‍💻 Author

 CPE032 - Big Data Engineering | CPE31S1
 Clavines, Miguel Arwyn
 Latonero, Vince Philip
 Peñas, John Patrick

---

**Built with ❤️ for Big Data Engineering Course**

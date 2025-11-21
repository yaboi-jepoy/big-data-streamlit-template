"""
Kafka Producer Template for Streaming Data Dashboard
STUDENT PROJECT: Big Data Streaming Data Producer

This is a template for students to build a Kafka producer that generates and sends
streaming data to Kafka for consumption by the dashboard.

DO NOT MODIFY THE TEMPLATE STRUCTURE - IMPLEMENT THE TODO SECTIONS
"""

import argparse
import json
import time
import random
import math
from datetime import datetime, timedelta
from typing import Dict, Any, List

# Kafka libraries
from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable

# avro imports
import avro.schema
import avro.io
import io as python_io

# for web functions
import requests


class StreamingDataProducer:
    """
    Enhanced Kafka producer with stateful synthetic data generation
    This class handles Kafka connection, realistic data generation, and message sending
    """
    
    def __init__(self, bootstrap_servers: str, topic: str):
        """
        Initialize Kafka producer configuration with stateful data generation
        
        Parameters:
        - bootstrap_servers: Kafka broker addresses (e.g., "localhost:9092")
        - topic: Kafka topic to produce messages to
        """
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        
        # Kafka producer configuration
        self.producer_config = {
            'bootstrap_servers': bootstrap_servers,
            # 'security_protocol': 'SSL',  # If using SSL
            # 'ssl_cafile': 'path/to/ca.pem',  # If using SSL
            # 'ssl_certfile': 'path/to/service.cert',  # If using SSL
            # 'ssl_keyfile': 'path/to/service.key',  # If using SSL
            # 'compression_type': 'gzip',  # Optional: Enable compression
            # 'batch_size': 16384,  # Optional: Tune batch size
            # 'linger_ms': 10,  # Optional: Wait for batch fill
            'acks': 'all',
            'retries': 3,
            'max_in_flight_requests_per_connection': 5,
            'compression_type': 'gzip',
            'linger_ms': 10,
        }
        
            # load avro schema
        try:
            schema_path = "sensor_schema.avsc"
            with open(schema_path, "r") as schema_file:
                schema_json = schema_file.read()
            self.avro_schema = avro.schema.parse(schema_json)
            print(f"✅ Avro schema loaded from {schema_path}")
        except FileNotFoundError:
            print(f"❌ ERROR: Schema file not found: {schema_path}")
            print("   Make sure sensor_schema.avsc is in the same directory as producer.py")
            self.avro_schema = None
        except Exception as e:
            print(f"❌ ERROR: Failed to parse Avro schema: {e}")
            self.avro_schema = None
        
        # ... rest of initialization code (sensors, states, etc.)
        
        # Initialize Kafka producer
        try:
            self.producer = KafkaProducer(**self.producer_config)
            print(f"✅ Kafka producer initialized for {bootstrap_servers} on topic {topic}")
        except Exception as e:
            print(f"❌ ERROR: Failed to initialize Kafka producer: {e}")
            self.producer = None
        
        # Stateful data generation attributes
        self.sensor_states = {}  # Track state for each sensor
        self.time_counter = 0    # Track time progression
        self.base_time = datetime.utcnow()
        
        # Expanded sensor pool with metadata
        self.sensors = [
            {"id": "sensor_001", "location": "server_room_a", "type": "temperature", "unit": "celsius"},
            {"id": "sensor_002", "location": "server_room_b", "type": "temperature", "unit": "celsius"},
            {"id": "sensor_003", "location": "outdoor_north", "type": "temperature", "unit": "celsius"},
            {"id": "sensor_004", "location": "lab_1", "type": "humidity", "unit": "percent"},
            {"id": "sensor_005", "location": "lab_2", "type": "humidity", "unit": "percent"},
            {"id": "sensor_006", "location": "control_room", "type": "pressure", "unit": "hPa"},
            {"id": "sensor_007", "location": "factory_floor", "type": "pressure", "unit": "hPa"},
            {"id": "sensor_008", "location": "warehouse", "type": "temperature", "unit": "celsius"},
            {"id": "sensor_009", "location": "office_area", "type": "humidity", "unit": "percent"},
            {"id": "sensor_010", "location": "basement", "type": "pressure", "unit": "hPa"},
        ]
        
        # Metric type configurations
        self.metric_ranges = {
            "temperature": {"min": -10, "max": 45, "daily_amplitude": 8, "trend_range": (-0.5, 0.5)},
            "humidity": {"min": 20, "max": 95, "daily_amplitude": 15, "trend_range": (-0.2, 0.2)},
            "pressure": {"min": 980, "max": 1040, "daily_amplitude": 5, "trend_range": (-0.1, 0.1)},
        }
        
        # Initialize Kafka producer
        try:
            self.producer = KafkaProducer(**self.producer_config)
            print(f"Kafka producer initialized for {bootstrap_servers} on topic {topic}")
        except NoBrokersAvailable:
            print(f"ERROR: No Kafka brokers available at {bootstrap_servers}")
            self.producer = None
        except Exception as e:
            print(f"ERROR: Failed to initialize Kafka producer: {e}")
            self.producer = None

    def generate_sample_data(self) -> Dict[str, Any]:
        """
        ✅ COMPLETED: Fetch real weather data from WeatherAPI.com
        
        Returns data for multiple cities in required schema format
        """
        
        # Your WeatherAPI key
        API_KEY = "b3438c26b1164aa7afb190426252011"
        BASE_URL = "http://api.weatherapi.com/v1/current.json"
        
        # List of cities to monitor (simulating multiple sensors)
        cities = [
            "Quezon City",
            "Manila", 
            "Makati",
            "Pasig",
            "Caloocan"
        ]
        
        # Rotate through cities to simulate multiple sensors
        if not hasattr(self, 'city_index'):
            self.city_index = 0
        
        city = cities[self.city_index % len(cities)]
        self.city_index += 1
        
        try:
            # Make API request
            url = f"{BASE_URL}?key={API_KEY}&q={city}&aqi=no"
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            
            weather_data = response.json()
            
            # Extract data from API response
            location = weather_data['location']['name']
            temp_c = weather_data['current']['temp_c']
            humidity = weather_data['current']['humidity']
            pressure_mb = weather_data['current']['pressure_mb']
            
            # Randomly choose which metric to send (temperature, humidity, or pressure)
            metric_choice = random.choice(['temperature', 'humidity', 'pressure'])
            
            if metric_choice == 'temperature':
                value = temp_c
                unit = 'celsius'
            elif metric_choice == 'humidity':
                value = humidity
                unit = 'percent'
            else:  # pressure
                value = pressure_mb
                unit = 'hPa'
            
            # Generate sensor ID based on city and metric
            sensor_id = f"sensor_{city.replace(' ', '_').lower()}_{metric_choice[:4]}"
            
            # Return in required schema format
            sample_data = {
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "value": round(value, 2),
                "metric_type": metric_choice,
                "sensor_id": sensor_id,
                "location": location,
                "unit": unit
            }
            
            return sample_data
            
        except requests.exceptions.RequestException as e:
            print(f"⚠️  API request error: {e}")
            # Fallback to synthetic data if API fails
            return self._generate_fallback_data()
        except KeyError as e:
            print(f"⚠️  API response parsing error: {e}")
            return self._generate_fallback_data()

    def _generate_fallback_data(self) -> Dict[str, Any]:
        """Fallback synthetic data if API fails"""
        sensor = random.choice(self.sensors)
        return {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "value": round(random.uniform(20, 30), 2),
            "metric_type": sensor["type"],
            "sensor_id": sensor["id"],
            "location": sensor["location"],
            "unit": sensor["unit"]
        }


    def serialize_data(self, data: Dict[str, Any]) -> bytes:
        """
        Convert data to Avro binary format
        """
        # Check if Avro schema is loaded
        if not self.avro_schema:
            print("❌ ERROR: Avro schema not loaded, falling back to JSON")
            try:
                return json.dumps(data).encode('utf-8')
            except Exception as e:
                print(f"❌ JSON fallback failed: {e}")
                return None
        
        try:
            # ⭐ Avro serialization goes HERE
            bytes_writer = python_io.BytesIO()
            encoder = avro.io.BinaryEncoder(bytes_writer)
            writer = avro.io.DatumWriter(self.avro_schema)
            writer.write(data, encoder)
            serialized_data = bytes_writer.getvalue()
            bytes_writer.close()
            
            return serialized_data
            
        except avro.io.AvroTypeException as e:
            print(f"❌ Avro schema validation error: {e}")
            print(f"   Data that failed: {data}")
            return None
            
        except TypeError as e:
            print(f"❌ Serialization error - Invalid data type: {e}")
            return None
            
        except Exception as e:
            print(f"❌ Unexpected Avro serialization error: {e}")
            return None
    
    def send_message(self, data: Dict[str, Any]) -> bool:
        """
        Send serialized message to Kafka
        """
        # Check if producer is initialized
        if not self.producer:
            print("❌ ERROR: Kafka producer not initialized")
            return False
        
        # ⭐ Call serialize_data() to get Avro bytes
        serialized_data = self.serialize_data(data)
        if not serialized_data:
            print("❌ ERROR: Serialization failed")
            return False
        
        try:
            # ⭐ Send the serialized bytes to Kafka
            future = self.producer.send(self.topic, value=serialized_data)
            result = future.get(timeout=10)
            
            # Success message
            print(f"✓ Message sent - Topic: {self.topic}, "
                f"Partition: {result.partition}, Offset: {result.offset}, "
                f"Sensor: {data['sensor_id']}, Type: {data['metric_type']}, "
                f"Value: {data['value']}{data['unit']}")
            return True
            
        except KafkaError as e:
            print(f"❌ Kafka send error: {e}")
            return False
        except Exception as e:
            print(f"❌ Unexpected error during send: {e}")
            return False


    def produce_stream(self, messages_per_second: float = 0.1, duration: int = None):
        """
        COMPLETED: Implement the main streaming loop with enhanced monitoring
        """

        print("=" * 70)
        print(f"🚀 Starting producer stream...")
        print(f"   Rate: {messages_per_second} msg/sec ({1/messages_per_second:.1f} second intervals)")
        print(f"   Duration: {duration or 'infinite (press Ctrl+C to stop)'}")
        print(f"   Topic: {self.topic}")
        print(f"   Server: {self.bootstrap_servers}")
        print("=" * 70)
        
        # Initialize tracking variables
        start_time = time.time()
        message_count = 0
        success_count = 0
        failure_count = 0
        
        try:
            while True:
                # Check if we've reached the duration limit
                if duration and (time.time() - start_time) >= duration:
                    print(f"\n⏱️  Reached duration limit of {duration} seconds")
                    break
                
                # Generate and send data with error tracking
                try:
                    data = self.generate_sample_data()
                    success = self.send_message(data)
                    
                    message_count += 1
                    if success:
                        success_count += 1
                    else:
                        failure_count += 1
                        print(f"⚠️  Message {message_count} failed to send")
                    
                except Exception as e:
                    failure_count += 1
                    message_count += 1
                    print(f"❌ Error processing message {message_count}: {e}")
                
                # add sleep to not flood
                sleep_time = 1.0 / messages_per_second
                time.sleep(sleep_time)
                
        except KeyboardInterrupt:
            print("\n" + "=" * 70)
            print("⏹️  Producer interrupted by user (Ctrl+C)")
            print("=" * 70)
            
        except Exception as e:
            print("\n" + "=" * 70)
            print(f"❌ CRITICAL ERROR in streaming loop: {e}")
            print(f"   Error type: {type(e).__name__}")
            print("=" * 70)
        
        # cleanup
        finally:
            self.close()
            print("✅ Producer stopped successfully")
            print("=" * 70)


def parse_arguments():
    """STUDENT TODO: Configure command-line arguments for flexibility"""
    parser = argparse.ArgumentParser(description='Kafka Streaming Data Producer')
    
    # STUDENT TODO: Add additional command-line arguments as needed
    parser.add_argument(
        '--bootstrap-servers',
        type=str,
        default='localhost:9092',
        help='Kafka bootstrap servers (default: localhost:9092)'
    )
    
    parser.add_argument(
        '--topic',
        type=str,
        default='sensor-data',  # changed to the topic name
        help='Kafka topic to produce to (default: sensor-data)'
    )
    
    parser.add_argument(
        '--rate',
        type=float,
        default=0.1,
        help='Messages per second (default: 0.1 for 10-second intervals)'
    )
    
    parser.add_argument(
        '--duration',
        type=int,
        default=None,
        help='Run duration in seconds (default: infinite)'
    )
    
    # STUDENT TODO: Add more arguments for your specific use case
    # add verbose for more information
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable verbose output with detailed logging'
    )
    # parser.add_argument('--data-type', choices=['temperature', 'humidity', 'financial'], default='temperature')
    
    return parser.parse_args()


def main():
    """
    DONER: Main execution flow with error handling
    
    Implementation Steps:
    1. Parse command-line arguments
    2. Initialize Kafka producer
    3. Start streaming data
    4. Handle graceful shutdown
    """
    
    print("=" * 70)
    print("KAFKA STREAMING DATA PRODUCER")
    print("   Big Data Dashboard Project")
    print("=" * 70)
    
    # Parse command-line arguments
    args = parse_arguments()
    
    # Show config if verbose mode enabled
    if args.verbose:
        print("\n📋 Configuration:")
        print(f"   ├─ Bootstrap servers: {args.bootstrap_servers}")
        print(f"   ├─ Topic: {args.topic}")
        print(f"   ├─ Rate: {args.rate} msg/sec")
        print(f"   └─ Duration: {args.duration or 'infinite'}\n")
    
    # Initialize producer
    producer = StreamingDataProducer(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic
    )
    
    # Check if producer initialized successfully
    if not producer.producer:
        print("\n Failed to initialize Kafka producer")
        print("Please check that Kafka is running at:", args.bootstrap_servers)
        return  # exict if connection fialed
    
    # Start producing stream
    try:
        producer.produce_stream(
            messages_per_second=args.rate,
            duration=args.duration
        )
    except KeyboardInterrupt:
        # This is already handled in produce_stream(), but catch here too
        print("\n Main interrupted")
    except Exception as e:
        # Better error handling
        print("\n" + "=" * 70)
        print(f"CRITICAL ERROR in main execution: {e}")
        print(f"Error type: {type(e).__name__}")
        print("=" * 70)
    finally:
        # Clean final message
        print("\n" + "=" * 70)
        print("Producer execution completed")
        print("=" * 70)


# COMPLETED: Testing Instructions
if __name__ == "__main__":
    """
    Entry point for the script.
    
    Run with: python producer.py
    See options: python producer.py --help
    """
    main()
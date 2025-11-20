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
        Generate realistic streaming data with stateful patterns
        
        This function creates continuously changing records with realistic patterns:
        - Daily cycles using sine waves
        - Gradual trends and realistic noise
        - Multiple metric types (temperature, humidity, pressure)
        - Temporal consistency with progressive timestamps
        
        Expected data format (must include these fields for dashboard compatibility):
        {
            "timestamp": "2023-10-01T12:00:00Z",  # ISO format timestamp
            "value": 123.45,                      # Numeric measurement value
            "metric_type": "temperature",         # Type of metric (temperature, humidity, etc.)
            "sensor_id": "sensor_001",            # Unique identifier for data source
            "location": "server_room_a",          # Sensor location
            "unit": "celsius",                    # Measurement unit
        }
        """
        
        # Select a random sensor from the expanded pool
        sensor = random.choice(self.sensors)
        sensor_id = sensor["id"]
        metric_type = sensor["type"]
        
        # Initialize sensor state if not exists
        if sensor_id not in self.sensor_states:
            config = self.metric_ranges[metric_type]
            base_value = random.uniform(config["min"], config["max"])
            trend = random.uniform(config["trend_range"][0], config["trend_range"][1])
            phase_offset = random.uniform(0, 2 * 3.14159)  # Random phase for daily cycle
            
            self.sensor_states[sensor_id] = {
                "base_value": base_value,
                "trend": trend,
                "phase_offset": phase_offset,
                "last_value": base_value,
                "message_count": 0
            }
        
        state = self.sensor_states[sensor_id]
        
        # Calculate progressive timestamp with configurable intervals
        current_time = self.base_time + timedelta(seconds=self.time_counter)
        self.time_counter += random.uniform(0.5, 2.0)  # Variable intervals for realism
        
        # Generate realistic value with patterns
        config = self.metric_ranges[metric_type]
        
        # Daily cycle using sine wave (24-hour period)
        hours_in_day = 24
        current_hour = current_time.hour + current_time.minute / 60
        daily_cycle = math.sin(2 * 3.14159 * current_hour / hours_in_day + state["phase_offset"])
        
        # Apply trend over time (slow drift)
        trend_effect = state["trend"] * (state["message_count"] / 100.0)
        
        # Add realistic noise (small random variations)
        noise = random.uniform(-config["daily_amplitude"] * 0.1, config["daily_amplitude"] * 0.1)
        
        # Calculate final value with bounds checking
        base_value = state["base_value"]
        daily_variation = daily_cycle * config["daily_amplitude"]
        raw_value = base_value + daily_variation + trend_effect + noise
        
        # Ensure value stays within reasonable bounds
        bounded_value = max(config["min"], min(config["max"], raw_value))
        
        # Update state
        state["last_value"] = bounded_value
        state["message_count"] += 1
        
        # Occasionally introduce small trend changes for realism
        if random.random() < 0.01:  # 1% chance per message
            state["trend"] = random.uniform(config["trend_range"][0], config["trend_range"][1])
        
        # Generate realistic data structure
        sample_data = {
            "timestamp": current_time.isoformat() + 'Z',
            "value": round(bounded_value, 2),
            "metric_type": metric_type,
            "sensor_id": sensor_id,
            "location": sensor["location"],
            "unit": sensor["unit"],
        }
        
        return sample_data

    def serialize_data(self, data: Dict[str, Any]) -> bytes:
        """
        Implement Avro data serialization
        
        Convert the data dictionary to Avro binary format for Kafka transmission.
        Avro provides better performance and schema validation compared to JSON.
        
        Parameters:
        - data: Dictionary containing the message data
        
        Returns:
        - bytes: Avro-serialized data, or None if serialization fails
        """
        
        # Check if Avro schema is loaded
        if not self.avro_schema:
            print("❌ ERROR: Avro schema not loaded, falling back to JSON")
            # Fallback to JSON if Avro schema failed to load
            try:
                return json.dumps(data).encode('utf-8')
            except Exception as e:
                print(f"❌ JSON fallback failed: {e}")
                return None
        
        try:
            # Step 1: Create a bytes buffer (like an in-memory file)
            bytes_writer = python_io.BytesIO()
            
            # Step 2: Create an Avro encoder that writes to the buffer
            encoder = avro.io.BinaryEncoder(bytes_writer)
            
            # Step 3: Create a DatumWriter that uses our schema
            writer = avro.io.DatumWriter(self.avro_schema)
            
            # Step 4: Write the data according to the schema
            writer.write(data, encoder)
            
            # Step 5: Get the serialized bytes from the buffer
            serialized_data = bytes_writer.getvalue()
            
            # Close the buffer
            bytes_writer.close()
            
            return serialized_data
            
        except avro.io.AvroTypeException as e:
            # This happens when data doesn't match the schema
            print(f"❌ Avro schema validation error: {e}")
            print(f"   Data that failed: {data}")
            return None
            
        except TypeError as e:
            # This happens if data contains non-serializable types
            print(f"❌ Serialization error - Invalid data type: {e}")
            print(f"   Problem data: {data}")
            return None
            
        except Exception as e:
            # Catch any other unexpected errors
            print(f"❌ Unexpected Avro serialization error: {e}")
            return None


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
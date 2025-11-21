"""
MongoDB Storage Consumer - Continuously saves Kafka messages to MongoDB
Run this alongside producer for historical data storage
"""

from kafka import KafkaConsumer
from pymongo import MongoClient
from datetime import datetime
import avro.schema
import avro.io
import io as python_io

class MongoDBStorageConsumer:
    def __init__(self):
        # Kafka settings
        self.kafka_topic = 'sensor-data'
        self.kafka_servers = 'localhost:9092'
        
        # MongoDB settings
        self.mongo_uri = 'mongodb://localhost:27017/'
        self.db_name = 'weather_dashboard'
        self.collection_name = 'sensor_readings'
        
        # Load Avro schema
        self.schema = self.load_avro_schema()
        
        # Initialize connections
        self.consumer = self.init_kafka()
        self.mongo_client, self.collection = self.init_mongodb()
    
    def load_avro_schema(self):
        """Load Avro schema"""
        try:
            with open("sensor_schema.avsc", "r") as f:
                return avro.schema.parse(f.read())
        except Exception as e:
            print(f"❌ Schema error: {e}")
            return None
    
    def init_kafka(self):
        """Initialize Kafka consumer"""
        try:
            consumer = KafkaConsumer(
                self.kafka_topic,
                bootstrap_servers=self.kafka_servers,
                auto_offset_reset='latest',  # Only new messages
                enable_auto_commit=True,
                group_id='mongodb-storage-consumer',
                value_deserializer=lambda m: m
            )
            print(f"✅ Kafka consumer connected: {self.kafka_servers} → {self.kafka_topic}")
            return consumer
        except Exception as e:
            print(f"❌ Kafka error: {e}")
            return None
    
    def init_mongodb(self):
        """Initialize MongoDB connection"""
        try:
            client = MongoClient(self.mongo_uri)
            client.admin.command('ping')
            
            db = client[self.db_name]
            collection = db[self.collection_name]
            
            # Create indexes for faster queries
            collection.create_index("timestamp")
            collection.create_index("location")
            collection.create_index("metric_type")
            collection.create_index("sensor_id")
            
            print(f"✅ MongoDB connected: {self.db_name}.{self.collection_name}")
            print(f"   Total documents: {collection.count_documents({})}")
            
            return client, collection
        except Exception as e:
            print(f"❌ MongoDB error: {e}")
            print("   Make sure MongoDB is running: sudo docker start mongodb")
            return None, None
    
    def deserialize_avro(self, avro_bytes):
        """Deserialize Avro message"""
        try:
            bytes_reader = python_io.BytesIO(avro_bytes)
            decoder = avro.io.BinaryDecoder(bytes_reader)
            reader = avro.io.DatumReader(self.schema)
            return reader.read(decoder)
        except Exception as e:
            print(f"❌ Deserialization error: {e}")
            return None
    
    def store_to_mongodb(self, data):
        """Store data to MongoDB"""
        if self.collection is None:
            return False

        
        try:
            # Add storage timestamp
            data['stored_at'] = datetime.utcnow()
            
            # Insert document
            result = self.collection.insert_one(data)
            return result.acknowledged
        except Exception as e:
            print(f"❌ MongoDB insert error: {e}")
            return False
    
    def run(self):
        """Main loop: consume from Kafka and store to MongoDB"""
        print("=" * 70)
        print("🗄️  MONGODB STORAGE CONSUMER STARTED")
        print("   Kafka → MongoDB")
        print("=" * 70)
        
        if self.consumer is None or self.collection is None:
            print("❌ Cannot start - consumer or MongoDB not initialized")
            return
        
        message_count = 0
        success_count = 0
        
        try:
            for message in self.consumer:
                try:
                    # Deserialize Avro message
                    data = self.deserialize_avro(message.value)
                    
                    if data:
                        # Store to MongoDB
                        if self.store_to_mongodb(data):
                            success_count += 1
                            message_count += 1
                            
                            print(f"✓ Stored #{success_count}: {data['location']} - "
                                  f"{data['metric_type']} = {data['value']}{data['unit']}")
                            
                            # Progress update every 10 messages
                            if success_count % 10 == 0:
                                print(f"\n📊 Total stored: {success_count}\n")
                        else:
                            message_count += 1
                            print(f"⚠️  Failed to store message #{message_count}")
                    
                except Exception as e:
                    print(f"❌ Error processing message: {e}")
                    continue
        
        except KeyboardInterrupt:
            print("\n" + "=" * 70)
            print("⏹️  Storage consumer stopped (Ctrl+C)")
            print("=" * 70)
        
        finally:
            print(f"\n📊 FINAL STATS:")
            print(f"   Total messages stored: {success_count}")
            
            if self.consumer:
                self.consumer.close()
                print("🔒 Kafka consumer closed")
            
            if self.mongo_client:
                self.mongo_client.close()
                print("🔒 MongoDB connection closed")
            
            print("=" * 70)

def main():
    consumer = MongoDBStorageConsumer()
    consumer.run()

if __name__ == "__main__":
    main()

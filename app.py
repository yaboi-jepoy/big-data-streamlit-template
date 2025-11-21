"""
Streaming Data Dashboard Template
STUDENT PROJECT: Big Data Streaming Dashboard

This is a template for students to build a real-time streaming data dashboard.
Students will need to implement the actual data processing, Kafka consumption,
and storage integration.

IMPLEMENT THE TODO SECTIONS
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import time
import json
from datetime import datetime, timedelta
from kafka import KafkaConsumer
from kafka.errors import KafkaError, NoBrokersAvailable
from streamlit_autorefresh import st_autorefresh

# avro imports
import avro.schema
import avro.io
import io as python_io

# Global: Load Avro schema
@st.cache_resource
def load_avro_schema():
    """Load Avro schema for deserialization"""
    try:
        with open("sensor_schema.avsc", "r") as f:
            schema_json = f.read()
        return avro.schema.parse(schema_json)
    except Exception as e:
        st.error(f"Failed to load Avro schema: {e}")
        return None

def deserialize_avro(avro_bytes, schema):
    """Deserialize Avro binary data"""
    try:
        bytes_reader = python_io.BytesIO(avro_bytes)
        decoder = avro.io.BinaryDecoder(bytes_reader)
        reader = avro.io.DatumReader(schema)
        return reader.read(decoder)
    except Exception as e:
        st.error(f"Avro deserialization error: {e}")
        return None


# Page configuration
st.set_page_config(
    page_title="Streaming Data Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

def setup_sidebar():
    """
    STUDENT TODO: Configure sidebar settings and controls
    Implement any configuration options students might need
    """
    st.sidebar.title("Dashboard Controls")
    
    # STUDENT TODO: Add configuration options for data sources
    st.sidebar.subheader("Data Source Configuration")
    
    # Placeholder for Kafka configuration
    kafka_broker = st.sidebar.text_input(
        "Kafka Broker", 
        value="localhost:9092",
        help="STUDENT TODO: Configure your Kafka broker address"
    )
    
    kafka_topic = st.sidebar.text_input(
        "Kafka Topic", 
        value="sensor-data",
        help="STUDENT TODO: Specify the Kafka topic to consume from"
    )
    
    # Placeholder for storage configuration
    st.sidebar.subheader("Storage Configuration")
    storage_type = st.sidebar.selectbox(
        "Storage Type",
        ["HDFS", "MongoDB"],
        help="STUDENT TODO: Choose your historical data storage solution"
    )
    
    return {
        "kafka_broker": kafka_broker,
        "kafka_topic": kafka_topic,
        "storage_type": storage_type
    }

def generate_sample_data():
    """
    STUDENT TODO: Replace this with actual data processing
    
    This function generates sample data for demonstration purposes.
    Students should replace this with real data from Kafka and storage systems.
    """
    # Sample data for demonstration - REPLACE WITH REAL DATA
    current_time = datetime.now()
    times = [current_time - timedelta(minutes=i) for i in range(100, 0, -1)]
    
    sample_data = pd.DataFrame({
        'timestamp': times,
        'value': [100 + i * 0.5 + (i % 10) for i in range(100)],
        'metric_type': ['temperature'] * 100,
        'sensor_id': ['sensor_1'] * 100
    })
    
    return sample_data

# @st.cache_resource
def consume_kafka_data(kafka_servers: str, kafka_topic: str, max_messages: int = 20):
    """
    Consume the NEWEST messages from Kafka (no offset tracking)
    """
    
    schema = load_avro_schema()
    if not schema:
        st.error("Cannot consume data without Avro schema")
        return []
    
    messages = []
    
    try:
        from kafka import TopicPartition
        import time as time_module
        
        consumer = KafkaConsumer(
            bootstrap_servers=kafka_servers,
            auto_offset_reset='earliest',
            enable_auto_commit=False,  
            consumer_timeout_ms=2000,
            value_deserializer=lambda m: m
        )
        
        # ⭐ Manually assign topic and partition
        partition = TopicPartition(kafka_topic, 0)
        consumer.assign([partition])
        consumer.seek_to_beginning(partition)
        
        st.success(f"✅ Connected to Kafka: {kafka_servers} → Topic: {kafka_topic}")
        
        # Read ALL messages
        all_messages = []
        for message in consumer:
            try:
                data = deserialize_avro(message.value, schema)
                if data:
                    all_messages.append(data)
            except:
                continue
        
        consumer.close()
        
        # Return LAST N messages (newest)
        if all_messages:
            messages = all_messages[-max_messages:]
            st.success(f"✅ Showing {len(messages)} newest messages (out of {len(all_messages)} total)")
        else:
            st.info("⏳ No messages in Kafka")
        
    except Exception as e:
        st.error(f"❌ Kafka error: {e}")
    
    return messages

def query_historical_data(storage_type: str, time_range: tuple = None, 
                         sensor_ids: list = None, metric_types: list = None):
    """
    PLACEHOLDER: Historical data from MongoDB (to be implemented later)
    
    For now, returns empty DataFrame
    """
    st.info("📝 Historical data view - MongoDB integration coming next!")
    st.write("For now, use the **Real-Time View** to see live Kafka data")
    return pd.DataFrame()


def display_real_time_view(data: pd.DataFrame):
    """Render real-time streaming view with improved charts"""
    st.header("📡 Real-Time Data Stream")
    
    if data.empty:
        st.warning("⚠️ No real-time data available")
        st.info("Make sure your producer is running: `python producer.py`")
        return
    
    # Convert timestamp and sort
    data['timestamp'] = pd.to_datetime(data['timestamp'], utc=True).dt.tz_convert('Asia/Manila')  # Use your timezone
    data = data.sort_values('timestamp', ascending=False)
    
    # Display metrics
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("Total Messages", len(data))
    
    with col2:
        st.metric("Unique Sensors", data['sensor_id'].nunique())
    
    with col3:
        latest_time = data['timestamp'].max()
        st.metric("Latest Update", str(latest_time)[:19])
    
    # Show latest messages table
    st.subheader("📋 Latest Messages (Newest First)")
    st.dataframe(
        data[['timestamp', 'sensor_id', 'metric_type', 'value', 'unit', 'location']].head(20),
        use_container_width=True
    )
    
    # ⭐ IMPROVED VISUALIZATIONS
    st.subheader("📊 Real-Time Weather Dashboard")
    
    # Get unique metric types
    metric_types = data['metric_type'].unique()
    
    # ⭐ Create columns for side-by-side comparison
    if len(metric_types) > 0:
        # 1. Current Values by Location (Bar Charts)
        st.subheader("🌍 Current Weather by Location")
        
        cols = st.columns(min(3, len(metric_types)))
        
        for idx, metric in enumerate(metric_types):
            with cols[idx % 3]:
                metric_data = data[data['metric_type'] == metric]
                
                # Get latest value for each location
                latest_by_location = metric_data.groupby('location').agg({
                    'value': 'last',
                    'unit': 'last',
                    'timestamp': 'max'
                }).reset_index()
                
                # Create bar chart
                fig = px.bar(
                    latest_by_location,
                    x='location',
                    y='value',
                    title=f"Current {metric.title()}",
                    labels={'value': f'{metric.title()} ({latest_by_location["unit"].iloc[0]})',
                           'location': 'City'},
                    color='value',
                    color_continuous_scale='RdYlBu_r' if metric == 'temperature' else 'Blues',
                    text='value'
                )
                fig.update_traces(texttemplate='%{text:.1f}', textposition='outside')
                fig.update_layout(showlegend=False, height=400)
                st.plotly_chart(fig, use_container_width=True)
        
        # 2. Time Series with Multiple Locations
        st.subheader("📈 Trends Over Time")
        
        for metric in metric_types:
            metric_data = data[data['metric_type'] == metric].sort_values('timestamp')
            
            if len(metric_data) > 1:  # Only show if we have multiple data points
                fig = px.line(
                    metric_data,
                    x='timestamp',
                    y='value',
                    color='location',
                    title=f"{metric.title()} Trend by City",
                    labels={'value': f'{metric.title()} ({metric_data["unit"].iloc[0]})',
                           'timestamp': 'Time',
                           'location': 'City'},
                    markers=True
                )
                fig.update_layout(
                    hovermode='x unified',
                    height=400,
                    xaxis_title="Time",
                    yaxis_title=f"{metric.title()} ({metric_data['unit'].iloc[0]})"
                )
                st.plotly_chart(fig, use_container_width=True)
        
        # 3. Comparison Table - Latest Values
        st.subheader("📊 Latest Weather Comparison")
        
        # Pivot table: rows=locations, columns=metrics
        pivot_data = data.groupby(['location', 'metric_type']).agg({
            'value': 'last',
            'timestamp': 'max'
        }).reset_index()
        
        comparison_table = pivot_data.pivot(
            index='location',
            columns='metric_type',
            values='value'
        ).round(2)
        
        # Style the dataframe
        # Show comparison table without styling
        st.dataframe(comparison_table, use_container_width=True)

        
        # 4. Statistics Summary
        st.subheader("📉 Statistical Summary")
        
        col1, col2, col3 = st.columns(3)
        
        for idx, metric in enumerate(metric_types):
            metric_data = data[data['metric_type'] == metric]
            
            with [col1, col2, col3][idx % 3]:
                st.markdown(f"**{metric.title()}** ({metric_data['unit'].iloc[0]})")
                
                stats_col1, stats_col2 = st.columns(2)
                with stats_col1:
                    st.metric("Min", f"{metric_data['value'].min():.1f}")
                    st.metric("Mean", f"{metric_data['value'].mean():.1f}")
                with stats_col2:
                    st.metric("Max", f"{metric_data['value'].max():.1f}")
                    st.metric("Std Dev", f"{metric_data['value'].std():.2f}")

def display_historical_view(config):
    """Render historical data view from MongoDB"""
    st.header("📊 Historical Weather Data")
    
    try:
        from pymongo import MongoClient
        from datetime import timedelta
        
        # Connect to MongoDB
        client = MongoClient('mongodb://localhost:27017/')
        db = client['weather_dashboard']
        collection = db['sensor_readings']
        
        # Check if we have data
        total_docs = collection.count_documents({})
        
        if total_docs == 0:
            st.info("📝 No historical data yet. Run storage_consumer.py to start collecting data!")
            st.code("python storage_consumer.py", language="bash")
            return
        
        st.success(f"✅ Connected to MongoDB - {total_docs} total readings")
        
        # Filters
        st.subheader("🔍 Filters")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            # Time range filter
            time_range = st.selectbox(
                "Time Range",
                ["Last Hour", "Last 6 Hours", "Last 24 Hours", "Last 7 Days", "All Time"]
            )
        
        with col2:
            # Get unique locations
            locations = collection.distinct("location")
            selected_location = st.selectbox("Location", ["All"] + locations)
        
        with col3:
            # Metric type filter
            metrics = collection.distinct("metric_type")
            selected_metric = st.selectbox("Metric Type", ["All"] + metrics)
        
        # Build query
        query = {}
        
        # Time filter
        if time_range != "All Time":
            hours_map = {
                "Last Hour": 1,
                "Last 6 Hours": 6,
                "Last 24 Hours": 24,
                "Last 7 Days": 168
            }
            hours = hours_map[time_range]
            cutoff_time = datetime.utcnow() - timedelta(hours=hours)
            query['timestamp'] = {'$gte': cutoff_time.isoformat()}
        
        # Location filter
        if selected_location != "All":
            query['location'] = selected_location
        
        # Metric filter
        if selected_metric != "All":
            query['metric_type'] = selected_metric
        
        # Query MongoDB
        cursor = collection.find(query).sort("timestamp", -1).limit(1000)
        results = list(cursor)
        
        if not results:
            st.warning("No data matches your filters")
            return
        
        # Convert to DataFrame
        df = pd.DataFrame(results)
        df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True).dt.tz_convert('Asia/Manila')
        
        # Display summary metrics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Total Readings", len(df))
        
        with col2:
            st.metric("Locations", df['location'].nunique())
        
        with col3:
            time_span = (df['timestamp'].max() - df['timestamp'].min()).total_seconds() / 3600
            st.metric("Time Span", f"{time_span:.1f}h")
        
        with col4:
            st.metric("Metrics", df['metric_type'].nunique())
            
        # EXPORT DATAAA
        st.subheader("💾 Export Data")

        # Prepare clean data for export
        export_df = df[['timestamp', 'location', 'sensor_id', 'metric_type', 'value', 'unit']].copy()

        # CSV Export
        csv_df = export_df.copy()
        csv_df['timestamp'] = csv_df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
        csv = csv_df.to_csv(index=False).encode('utf-8')

        st.download_button(
            label="📄 Download CSV",
            data=csv,
            file_name=f"weather_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv"
        )

        # JSON Export
        json_df = export_df.copy()
        json_df['timestamp'] = json_df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
        json_str = json_df.to_json(orient='records', indent=2)

        st.download_button(
            label="📋 Download JSON",
            data=json_str,
            file_name=f"weather_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
            mime="application/json"
        )

        # Excel Export
        try:
            from io import BytesIO
            
            excel_df = export_df.copy()
            excel_df['timestamp'] = pd.to_datetime(excel_df['timestamp']).dt.tz_localize(None)
            
            buffer = BytesIO()
            with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                excel_df.to_excel(writer, sheet_name='Weather Data', index=False)
            
            st.download_button(
                label="📊 Download Excel",
                data=buffer.getvalue(),
                file_name=f"weather_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        except Exception as e:
            st.error(f"Excel export error: {e}")

        st.divider()
        
        # Charts
        st.subheader("📈 Historical Trends")

        st.subheader("📦 Distribution Analysis")

        col1, col2 = st.columns(2)

        for idx, metric in enumerate(df['metric_type'].unique()):
            metric_data = df[df['metric_type'] == metric]
            
            with [col1, col2][idx % 2]:
                fig = px.box(
                    metric_data,
                    x='location',
                    y='value',
                    color='location',
                    title=f"{metric.title()} Distribution by City",
                    labels={'value': f'{metric.title()} ({metric_data["unit"].iloc[0]})'}
                )
                fig.update_layout(showlegend=False, height=400)
                st.plotly_chart(fig, use_container_width=True)

        st.subheader("⏰ Average Readings by Time of Day")

        df['hour'] = df['timestamp'].dt.hour

        cols = st.columns(min(3, df['metric_type'].nunique()))

        for idx, metric in enumerate(df['metric_type'].unique()):
            metric_data = df[df['metric_type'] == metric]
            
            hourly_avg = metric_data.groupby('hour').agg({
                'value': 'mean'
            }).reset_index()
            
            with cols[idx % 3]:
                fig = px.bar(
                    hourly_avg,
                    x='hour',
                    y='value',
                    title=f"Avg {metric.title()} by Hour",
                    labels={'hour': 'Hour of Day', 'value': f'{metric.title()}'},
                    color='value',
                    color_continuous_scale='Viridis'
                )
                fig.update_layout(showlegend=False, height=350)
                st.plotly_chart(fig, use_container_width=True)
                
        # Group by metric type
        for metric in df['metric_type'].unique():
            metric_data = df[df['metric_type'] == metric].sort_values('timestamp')
            
            fig = px.line(
                metric_data,
                x='timestamp',
                y='value',
                color='location',
                title=f"{metric.title()} Over Time",
                labels={'value': f'{metric.title()} ({metric_data["unit"].iloc[0]})',
                       'timestamp': 'Time'},
                markers=True
            )
            fig.update_layout(hovermode='x unified', height=400)
            st.plotly_chart(fig, use_container_width=True)
        
        # Data table
        st.subheader("📋 Recent Readings")
        st.dataframe(
            df[['timestamp', 'location', 'metric_type', 'value', 'unit', 'sensor_id']],
            use_container_width=True
        )
        
        client.close()
        
    except Exception as e:
        st.error(f"❌ Error loading historical data: {e}")
        st.info("Make sure MongoDB is running: sudo docker start mongodb")


def main():
    """
    STUDENT TODO: Customize the main application flow as needed
    """
    st.title("CPL Weather Monitor")
        
    with st.expander("📋 Project Instructions"):
        st.markdown("""
        **STUDENT PROJECT TEMPLATE**
        
        ### Implementation Required:
        - **Real-time Data**: Connect to Kafka and process streaming data
        - **Historical Data**: Query from HDFS/MongoDB
        - **Visualizations**: Create meaningful charts
        - **Error Handling**: Implement robust error handling
        """)
    
    # Initialize session state for refresh management
    if 'refresh_state' not in st.session_state:
        st.session_state.refresh_state = {
            'last_refresh': datetime.now(),
            'auto_refresh': True
        }
    
    # Setup configuration
    config = setup_sidebar()
    
    # Refresh controls in sidebar
    st.sidebar.subheader("Refresh Settings")
    st.session_state.refresh_state['auto_refresh'] = st.sidebar.checkbox(
        "Enable Auto Refresh",
        value=st.session_state.refresh_state['auto_refresh'],
        help="Automatically refresh real-time data"
    )
    
    if st.session_state.refresh_state['auto_refresh']:
        refresh_interval = st.sidebar.slider(
            "Refresh Interval (seconds)",
            min_value=5,
            max_value=60,
            value=15,
            help="Set how often real-time data refreshes"
        )
        
        # Auto-refresh using streamlit-autorefresh package
        st_autorefresh(interval=refresh_interval * 1000, key="auto_refresh")
    
    # Manual refresh button
    if st.sidebar.button("🔄 Manual Refresh"):
        st.session_state.refresh_state['last_refresh'] = datetime.now()
        st.rerun()
    
    # Display refresh status
    st.sidebar.markdown("---")
    st.sidebar.metric("Last Refresh", st.session_state.refresh_state['last_refresh'].strftime("%H:%M:%S"))
    
    # Create tabs for different views
    tab1, tab2 = st.tabs(["📈 Real-time Streaming", "📊 Historical Data"])
    
    with tab1:
        # Consume Kafka data and display
        messages = consume_kafka_data(config['kafka_broker'], config['kafka_topic'])
        df = pd.DataFrame(messages) if messages else pd.DataFrame()
        display_real_time_view(df)

    
    with tab2:
        display_historical_view(config)
    

if __name__ == "__main__":
    main()
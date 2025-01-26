import streamlit as st
import logging
from logger_config import setup_logger
from database_connector import SybaseDatabaseConnector
from spark_analyzer import SparkTableAnalyzer
import plotly.graph_objects as go

def main():
    # Setup logging
    logger = setup_logger()
    
    st.title("Sybase Database Table Analyzer")
    
    # Database Connection Parameters
    with st.form(key='connection_form'):
        col1, col2 = st.columns(2)
        
        with col1:
            hostname = st.text_input("Hostname", value="localhost")
            database = st.text_input("Database Name")
            num_workers = st.number_input("Number of PySpark Workers", min_value=1, max_value=10, value=2)
        
        with col2:
            port = st.number_input("Port", value=5000)
            username = st.text_input("Username")
            password = st.text_input("Password", type="password")
        
        submit_connection = st.form_submit_button("Connect")
    
    if submit_connection:
        try:
            # Establish Database Connection
            connector = SybaseDatabaseConnector(
                logger, hostname, port, database, username, password
            )
            connector.connect()
            
            # Retrieve Tables
            tables = connector.get_tables()
            
            st.success(f"Connected! Found {len(tables)} tables.")
            
            # Table Selection
            selected_tables = st.multiselect("Select Tables to Analyze", tables)
            
            if st.button("Analyze Selected Tables"):
                # Initialize Spark Analyzer
                spark_analyzer = SparkTableAnalyzer(logger, num_workers)
                
                # Analyze Tables
                results = spark_analyzer.analyze_tables(connector, selected_tables)
                
                # Display Results
                for result in results:
                    st.subheader(f"Table: {result['table_name']}")
                    
                    # Metadata Display
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("Row Count", result['row_count'])
                    with col2:
                        st.metric("Table Size", result['table_size'])
                    with col3:
                        st.metric("Primary Keys", ", ".join(result['primary_keys']))
                    
                    # Column Type Distribution
                    st.subheader("Column Type Distribution")
                    column_types = result['column_types']
                    
                    # Plotly Pie Chart for Column Types
                    type_counts = {}
                    for dtype in column_types.values():
                        type_counts[dtype] = type_counts.get(dtype, 0) + 1
                    
                    fig = go.Figure(data=[go.Pie(
                        labels=list(type_counts.keys()),
                        values=list(type_counts.values())
                    )])
                    st.plotly_chart(fig)
        
        except Exception as e:
            st.error(f"Error: {str(e)}")
            logger.error(f"Application error: {str(e)}")

if __name__ == "__main__":
    main()

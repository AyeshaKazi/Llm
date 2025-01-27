import streamlit as st
import logging
from logger_config import setup_logger
from database_connector import SybaseDatabaseConnector
from spark_analyzer import SparkTableAnalyzer
import plotly.graph_objects as go

def main():
    logger = setup_logger()
    st.title("Sybase Database Table Analyzer")
    
    # Initialize session state
    if 'connector' not in st.session_state:
        st.session_state.connector = None
        st.session_state.tables = []
        st.session_state.results = []
    
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
            connector = SybaseDatabaseConnector(logger, hostname, port, database, username, password)
            connector.connect()
            st.session_state.connector = connector
            st.session_state.tables = connector.get_tables()
            st.success(f"Connected! Found {len(st.session_state.tables)} tables.")
        except Exception as e:
            st.error(f"Error: {str(e)}")
            logger.error(f"Application error: {str(e)}")
    
    if st.session_state.connector:
        selected_tables = st.multiselect("Select Tables to Analyze", st.session_state.tables)
        
        if st.button("Analyze Selected Tables"):
            if not selected_tables:
                st.warning("Please select at least one table.")
                return
            
            try:
                spark_analyzer = SparkTableAnalyzer(logger, num_workers)
                results = spark_analyzer.analyze_tables(st.session_state.connector, selected_tables)
                st.session_state.results = results  # Store results in session state
                
                # Display results and column selection for each table
                for idx, result in enumerate(results):
                    st.subheader(f"Table: {result['table_name']}")
                    
                    # Initialize column selection in session state if not present
                    if f'selected_columns_{idx}' not in st.session_state:
                        st.session_state[f'selected_columns_{idx}'] = []
                    
                    col1, col2 = st.columns(2)
                    with col1:
                        st.metric("Row Count", int(result['row_count']))
                    
                    with col2:
                        # Get available columns for this table
                        available_columns = result.get('column_names', [])  # Make sure your spark_analyzer returns this
                        
                        # Store column selection in session state and results
                        selected_columns = st.multiselect(
                            "Select Columns to partition by",
                            available_columns,
                            key=f'selected_columns_{idx}'
                        )
                        
                        # Update the results with selected columns
                        st.session_state.results[idx]['selected_columns'] = selected_columns
                        
                    # Display column type distribution if available
                    if 'column_types' in result:
                        st.subheader("Column Type Distribution")
                        type_counts = {}
                        for dtype in result['column_types'].values():
                            type_counts[dtype] = type_counts.get(dtype, 0) + 1
                        
                        fig = go.Figure(data=[go.Pie(
                            labels=list(type_counts.keys()),
                            values=list(type_counts.values())
                        )])
                        st.plotly_chart(fig)
            
            except Exception as e:
                st.error(f"Error: {str(e)}")
                logger.error(f"Analysis error: {str(e)}")
            
            # You can now access the selected columns from st.session_state.results
            if st.button("Show Selected Columns"):
                for result in st.session_state.results:
                    st.write(f"Table: {result['table_name']}")
                    st.write(f"Selected columns: {result.get('selected_columns', [])}")

if __name__ == "__main__":
    main()

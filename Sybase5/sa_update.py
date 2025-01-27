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
        st.session_state.analysis_done = False
    
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
                st.session_state.results = spark_analyzer.analyze_tables(st.session_state.connector, selected_tables)
                st.session_state.analysis_done = True
                
            except Exception as e:
                st.error(f"Error: {str(e)}")
                logger.error(f"Analysis error: {str(e)}")
        
        # Display results only if analysis has been done
        if st.session_state.analysis_done and st.session_state.results:
            for idx, result in enumerate(st.session_state.results):
                st.subheader(f"Table: {result['table_name']}")
                
                # Initialize column selection in session state if not present
                if f'selected_columns_{idx}' not in st.session_state:
                    st.session_state[f'selected_columns_{idx}'] = []
                
                col1, col2 = st.columns(2)
                with col1:
                    st.metric("Row Count", int(result['row_count']))
                    st.metric("Table Size", result['table_size'])
                    st.metric("Primary Keys", ", ".join(result['primary_keys']))
                
                with col2:
                    # Get columns from metadata
                    available_columns = result['metadata'].get('column_names', [])
                    
                    # Store column selection in session state
                    selected_columns = st.multiselect(
                        "Select Columns to partition by",
                        options=available_columns,
                        key=f'table_{idx}_columns'
                    )
                    
                    # Update the results with selected columns
                    st.session_state.results[idx]['selected_columns'] = selected_columns
                
                # Display column type distribution
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
                
                # Display selected columns for debugging
                if selected_columns:
                    st.write("Currently selected columns:", selected_columns)

if __name__ == "__main__":
    main()

import streamlit as st
import plotly.express as px
import plotly.graph_objs as go
import pandas as pd
import sys
import logging
sys.path.append('.')  # Add current directory to path
from sybase_connection import SybaseTableAnalyzer

def main():
    st.title("Sybase Table Analysis Dashboard")
    
    # Sidebar for connection details
    st.sidebar.header("Sybase Connection Details")
    jdbc_url = st.sidebar.text_input("JDBC URL", "jdbc:sybase:Tds:hostname:port/dbname")
    username = st.sidebar.text_input("Username")
    password = st.sidebar.text_password("Password")
    database_name = st.sidebar.text_input("Database Name")

    # Initialize session state for selected tables
    if 'selected_tables' not in st.session_state:
        st.session_state.selected_tables = []

    if st.sidebar.button("Connect and List Tables"):
        try:
            # Initialize analyzer
            analyzer = SybaseTableAnalyzer(
                jdbc_url=jdbc_url, 
                username=username, 
                password=password
            )
            
            # Get tables
            tables = analyzer.get_database_tables(database_name)
            
            # Display tables with multiselect
            st.session_state.selected_tables = st.multiselect(
                "Select Tables for Analysis", 
                tables
            )
            
            # Store analyzer in session state for later use
            st.session_state.analyzer = analyzer
            st.session_state.database_name = database_name
        
        except Exception as e:
            st.error(f"An error occurred: {e}")

    # Analyze selected tables
    if st.session_state.get('selected_tables'):
        st.header("Selected Tables Analysis")
        
        # Tabs for each selected table
        tabs = st.tabs(st.session_state.selected_tables)
        
        # Analyze each selected table
        for i, table in enumerate(st.session_state.selected_tables):
            with tabs[i]:
                try:
                    # Retrieve analyzer from session state
                    analyzer = st.session_state.get('analyzer')
                    database_name = st.session_state.get('database_name')
                    
                    # Analyze the table
                    table_metrics = analyzer.analyze_table(database_name, table)
                    transfer_metrics = analyzer.estimate_transfer_time(table_metrics)
                    
                    # Display metrics in columns
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.subheader("Basic Metrics")
                        st.write(f"**Row Count:** {table_metrics.get('row_count', 'N/A')}")
                        st.write(f"**Columns:** {len(table_metrics.get('columns', []))}")
                        
                        st.subheader("Transfer Metrics")
                        st.write(f"**Estimated Size:** {transfer_metrics.get('estimated_size_mb', 'N/A')} MB")
                        st.write(f"**Estimated Transfer Time:** {transfer_metrics.get('estimated_transfer_time_minutes', 'N/A')} minutes")
                    
                    with col2:
                        st.subheader("Potential Partition Keys")
                        partition_keys = table_metrics.get('potential_partition_keys', [])
                        if partition_keys:
                            partition_df = pd.DataFrame(partition_keys)
                            st.dataframe(partition_df)
                        else:
                            st.write("No recommended partition keys found")
                    
                    # Visualizations
                    col3, col4 = st.columns(2)
                    
                    with col3:
                        # Null Count Visualization
                        st.subheader("Null Counts")
                        null_counts = table_metrics.get('null_counts', {})
                        null_df = pd.DataFrame.from_dict(null_counts, orient='index', columns=['Null Count'])
                        null_df.index.name = 'Column'
                        null_df = null_df.reset_index()
                        
                        if not null_df.empty:
                            fig_nulls = px.bar(
                                null_df, 
                                x="Column", 
                                y="Null Count", 
                                title="Null Counts by Column"
                            )
                            st.plotly_chart(fig_nulls)
                    
                    with col4:
                        # Column Types Visualization
                        st.subheader("Column Types")
                        column_types = table_metrics.get('column_types', {})
                        types_df = pd.DataFrame.from_dict(column_types, orient='index', columns=['Data Type'])
                        types_df.index.name = 'Column'
                        types_df = types_df.reset_index()
                        
                        if not types_df.empty:
                            fig_types = px.pie(
                                types_df, 
                                names="Column", 
                                values=[1]*len(types_df), 
                                title="Column Types Distribution"
                            )
                            st.plotly_chart(fig_types)
                
                except Exception as e:
                    st.error(f"Error analyzing table {table}: {e}")

if __name__ == "__main__":
    main()

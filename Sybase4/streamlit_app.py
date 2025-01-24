import streamlit as st
import plotly.express as px
import plotly.graph_objs as go
import pandas as pd
import sys
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

    if st.sidebar.button("Connect and Analyze"):
        try:
            # Initialize analyzer
            analyzer = SybaseTableAnalyzer(
                jdbc_url=jdbc_url, 
                username=username, 
                password=password
            )
            
            # Get tables
            tables = analyzer.get_database_tables(database_name)
            
            # Analyze each table
            table_metrics = {}
            for table in tables:
                metrics = analyzer.analyze_table(database_name, table)
                transfer_metrics = analyzer.estimate_transfer_time(metrics)
                metrics['transfer_metrics'] = transfer_metrics
                table_metrics[table] = metrics
            
            # Display Tables Overview
            st.header("Tables Overview")
            overview_data = []
            for table, metrics in table_metrics.items():
                overview_data.append({
                    "Table": table,
                    "Row Count": metrics.get('row_count', 0),
                    "Size (MB)": metrics.get('transfer_metrics', {}).get('estimated_size_mb', 0),
                    "Transfer Time (min)": metrics.get('transfer_metrics', {}).get('estimated_transfer_time_minutes', 0)
                })
            
            overview_df = pd.DataFrame(overview_data)
            st.dataframe(overview_df)
            
            # Visualizations
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("Row Count by Table")
                fig_rows = px.bar(
                    overview_df, 
                    x="Table", 
                    y="Row Count", 
                    title="Row Count Comparison"
                )
                st.plotly_chart(fig_rows)
            
            with col2:
                st.subheader("Transfer Time by Table")
                fig_transfer = px.bar(
                    overview_df, 
                    x="Table", 
                    y="Transfer Time (min)", 
                    title="Estimated Transfer Time"
                )
                st.plotly_chart(fig_transfer)
            
            # Detailed Table Analysis
            st.header("Detailed Table Analysis")
            selected_table = st.selectbox("Select Table for Detailed View", tables)
            
            if selected_table:
                table_data = table_metrics[selected_table]
                
                st.subheader(f"Details for {selected_table}")
                col1, col2 = st.columns(2)
                
                with col1:
                    st.write("**Basic Metrics:**")
                    st.json({
                        "Row Count": table_data.get('row_count', 'N/A'),
                        "Columns": table_data.get('columns', []),
                        "Estimated Size (MB)": table_data.get('transfer_metrics', {}).get('estimated_size_mb', 'N/A')
                    })
                
                with col2:
                    st.write("**Transfer Metrics:**")
                    st.json(table_data.get('transfer_metrics', {}))
                
                # Null Count Visualization
                null_counts = table_data.get('null_counts', {})
                null_df = pd.DataFrame.from_dict(null_counts, orient='index', columns=['Null Count'])
                null_df.index.name = 'Column'
                null_df = null_df.reset_index()
                
                st.subheader("Null Counts by Column")
                fig_nulls = px.bar(
                    null_df, 
                    x="Column", 
                    y="Null Count", 
                    title="Null Counts in Columns"
                )
                st.plotly_chart(fig_nulls)
            
            # Clean up
            analyzer.close()
        
        except Exception as e:
            st.error(f"An error occurred: {e}")

if __name__ == "__main__":
    main()

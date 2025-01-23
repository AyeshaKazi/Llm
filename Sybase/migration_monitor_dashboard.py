import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objs as go
import json
import os
from datetime import datetime

class MigrationDashboard:
    def __init__(self):
        self.migration_status_files = self._get_migration_status_files()

    def _get_migration_status_files(self):
        """Find all migration status JSON files"""
        return [f for f in os.listdir('.') if f.startswith('migration_status_') and f.endswith('.json')]

    def load_migration_status(self, filename):
        """Load migration status from JSON file"""
        with open(filename, 'r') as f:
            return json.load(f)

    def read_log_file(self, log_filename):
        """Read migration log file"""
        try:
            with open(log_filename, 'r') as f:
                return f.readlines()
        except FileNotFoundError:
            return []

    def display_dashboard(self):
        st.title('Data Migration Monitoring Dashboard')

        # Sidebar for file selection
        selected_file = st.sidebar.selectbox(
            'Select Migration Run', 
            self.migration_status_files
        )

        # Load migration status
        migration_status = self.load_migration_status(selected_file)
        log_filename = f"migration_{selected_file.replace('status_', '').replace('.json', '')}.log"

        # Metrics Section
        col1, col2, col3 = st.columns(3)
        col1.metric("Total Records", migration_status.get('total_records', 'N/A'))
        col2.metric("Total Size (MB)", migration_status.get('total_size_mb', 'N/A'))
        col3.metric("Partitions", migration_status.get('partitions', 'N/A'))

        # Progress Bar
        progress = (migration_status.get('s3_upload_success', 0) / migration_status.get('partitions', 1)) * 100
        st.progress(int(progress))
        st.write(f"Migration Progress: {progress:.2f}%")

        # Errors Log
        st.subheader('Error Logs')
        error_logs = [line for line in self.read_log_file(log_filename) if 'ERROR' in line]
        if error_logs:
            st.error('\n'.join(error_logs))
        else:
            st.success('No errors detected')

        # Partition Progress Visualization
        st.subheader('Partition Progress')
        if migration_status.get('partitions'):
            partition_df = pd.DataFrame([
                {'Partition': f'Partition {i+1}', 'Records': part.get('records_count', 0)} 
                for i, part in enumerate(migration_status.get('partitions', []))
            ])
            
            fig = px.bar(
                partition_df, 
                x='Partition', 
                y='Records', 
                title='Records per Partition'
            )
            st.plotly_chart(fig)

def main():
    dashboard = MigrationDashboard()
    dashboard.display_dashboard()

if __name__ == '__main__':
    main()

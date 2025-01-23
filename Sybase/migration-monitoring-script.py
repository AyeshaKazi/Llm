import os
import json
import time
from datetime import datetime, timedelta

class MigrationMonitor:
    def __init__(self, status_file, log_file):
        self.status_file = status_file
        self.log_file = log_file

    def get_migration_status(self):
        """Read migration status from JSON file"""
        try:
            with open(self.status_file, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            return None
        
    def read_log_file(self):
        """Read migration log file"""
        try:
            with open(self.log_file, 'r') as f:
                return f.readlines()
        except FileNotFoundError:
            return []

    def calculate_progress(self):
        """Calculate migration progress percentage"""
        status = self.get_migration_status()
        if not status:
            return 0
        
        # Example progress calculation (adjust based on actual metrics)
        total_records = status.get('total_records', 0)
        uploaded_records = sum(
            partition['records_count'] 
            for partition in status.get('partitions', [])
        )
        
        progress_percentage = (uploaded_records / total_records) * 100 if total_records > 0 else 0
        return progress_percentage

    def monitor_errors(self):
        """Extract error logs"""
        log_lines = self.read_log_file()
        errors = [line for line in log_lines if 'ERROR' in line]
        return errors

    def generate_detailed_report(self):
        """Generate comprehensive migration report"""
        status = self.get_migration_status()
        errors = self.monitor_errors()
        
        report = {
            'overall_status': 'In Progress' if status else 'Not Started',
            'progress_percentage': self.calculate_progress(),
            'total_records': status.get('total_records', 0),
            'total_size_mb': status.get('total_size_mb', 0),
            'partitions_completed': len(status.get('partitions', [])) if status else 0,
            'errors': errors
        }
        
        return report

def continuous_monitor(status_file, log_file, check_interval=300):
    """Monitor migration progress continuously"""
    monitor = MigrationMonitor(status_file, log_file)
    
    while True:
        report = monitor.generate_detailed_report()
        print(json.dumps(report, indent=2))
        
        if report['overall_status'] == 'Completed' or report['progress_percentage'] == 100:
            break
        
        time.sleep(check_interval)  # Wait 5 minutes between checks

if __name__ == '__main__':
    # Example usage
    latest_migration_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    status_file = f'migration_status_{latest_migration_id}.json'
    log_file = f'migration_{latest_migration_id}.log'
    
    continuous_monitor(status_file, log_file)

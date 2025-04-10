#!/usr/bin/env python3
import subprocess
import sys
import os
import schedule
import time

def run_tasks():
    # Change directory to the plus_cron folder on your Desktop
    folder_path = os.path.expanduser("~/Desktop/plus_cron")
    try:
        os.chdir(folder_path)
    except FileNotFoundError:
        print(f"Directory not found: {folder_path}", file=sys.stderr)
        sys.exit(1)

    # Run the crawler file
    print("Running crawler...")
    result = subprocess.run(["./crawler.py"])
    
    # Check if crawler executed successfully
    if result.returncode == 0:
        print("Crawler completed successfully, starting parser...")
        subprocess.run(["./parser.py"])
    else:
        print("Crawler encountered an error. Parser will not run.", file=sys.stderr)

def main():
    # Set the desired schedule time directly in the code (HH:MM 24-hour format)
    schedule_time = "15:30"  # Change this value to your required time

    print(f"Scheduling tasks daily at {schedule_time}...")
    schedule.every().day.at(schedule_time).do(run_tasks)

    # Optional: Run the task once immediately (uncomment if desired)
    # run_tasks()

    # Loop to keep the scheduler running.
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    main()

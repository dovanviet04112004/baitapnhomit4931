"""
Batch Processing Scheduler
Tự động chạy batch_processing.py theo lịch định kỳ

Usage:
  python scheduler.py              # Chạy scheduler (mặc định mỗi 4 giờ)
  python scheduler.py --interval 6 # Chạy mỗi 6 giờ
  python scheduler.py --once       # Chạy 1 lần rồi thoát
"""

import subprocess
import sys
import time
import argparse
from datetime import datetime
from pathlib import Path

# Paths
SCRIPT_DIR = Path(__file__).resolve().parent
BATCH_SCRIPT = SCRIPT_DIR / "batch_processing.py"
LOG_DIR = SCRIPT_DIR / "logs"


def run_batch_job(mode="incremental"):
    """Chạy batch processing job"""
    LOG_DIR.mkdir(exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = LOG_DIR / f"batch_{timestamp}.log"
    
    print(f"\n{'='*60}")
    print(f"🚀 [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Starting batch job ({mode} mode)")
    print(f"{'='*60}")
    
    try:
        # Chạy batch_processing.py với mode
        cmd = [sys.executable, str(BATCH_SCRIPT), f"--{mode}"]
        
        with open(log_file, "w", encoding="utf-8") as f:
            result = subprocess.run(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8"
            )
            f.write(result.stdout)
            print(result.stdout)
        
        if result.returncode == 0:
            print(f"✅ Job completed successfully!")
            print(f"📄 Log saved to: {log_file}")
            return True
        else:
            print(f"❌ Job failed with code {result.returncode}")
            print(f"📄 Check log: {log_file}")
            return False
            
    except Exception as e:
        print(f"❌ Error running job: {e}")
        return False


def run_scheduler(interval_hours=4):
    """Chạy scheduler loop"""
    print(f"""
╔══════════════════════════════════════════════════════════╗
║       CRYPTO BATCH PROCESSING SCHEDULER                  ║
║                                                          ║
║  Interval: Every {interval_hours} hours                              ║
║  Mode: Incremental (chỉ xử lý data mới)                  ║
║                                                          ║
║  Press Ctrl+C to stop                                    ║
╚══════════════════════════════════════════════════════════╝
    """)
    
    job_count = 0
    
    while True:
        job_count += 1
        print(f"\n📊 Job #{job_count}")
        
        success = run_batch_job(mode="incremental")
        
        if success:
            next_run = datetime.now().timestamp() + (interval_hours * 3600)
            next_run_str = datetime.fromtimestamp(next_run).strftime("%Y-%m-%d %H:%M:%S")
            print(f"\n⏰ Next run at: {next_run_str}")
            print(f"💤 Sleeping for {interval_hours} hours...")
            
            try:
                time.sleep(interval_hours * 3600)
            except KeyboardInterrupt:
                print("\n\n🛑 Scheduler stopped by user")
                break
        else:
            print("\n⚠️ Job failed, retrying in 5 minutes...")
            try:
                time.sleep(300)  # Retry after 5 minutes if failed
            except KeyboardInterrupt:
                print("\n\n🛑 Scheduler stopped by user")
                break


def main():
    parser = argparse.ArgumentParser(description="Batch Processing Scheduler")
    parser.add_argument("--interval", type=int, default=6, 
                        help="Interval between runs in hours (default: 6)")
    parser.add_argument("--once", action="store_true",
                        help="Run once and exit (full mode)")
    parser.add_argument("--incremental-once", action="store_true",
                        help="Run once in incremental mode and exit")
    
    args = parser.parse_args()
    
    if args.once:
        # Chạy 1 lần full mode
        run_batch_job(mode="full")
    elif args.incremental_once:
        # Chạy 1 lần incremental mode
        run_batch_job(mode="incremental")
    else:
        # Chạy scheduler loop
        run_scheduler(interval_hours=args.interval)


if __name__ == "__main__":
    main()

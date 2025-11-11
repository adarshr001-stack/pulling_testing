import psutil
import subprocess
import time
import threading
import os

def bytes_to_mb(b):
    return b / (1024 * 1024)

def monitor(pid, results):
    """Monitor CPU, memory, and network usage for the given PID."""
    proc = psutil.Process(pid)

    net_start = psutil.net_io_counters()
    start_time = time.time()

    cpu_peaks = []
    mem_peaks = []
    net_peaks = []

    try:
        while proc.is_running():
            cpu = proc.cpu_percent(interval=1)
            mem = proc.memory_info().rss / (1024 * 1024)  # MB
            net_current = psutil.net_io_counters()
            net_sent = net_current.bytes_sent - net_start.bytes_sent
            net_recv = net_current.bytes_recv - net_start.bytes_recv
            net_total = bytes_to_mb(net_sent + net_recv)

            cpu_peaks.append(cpu)
            mem_peaks.append(mem)
            net_peaks.append(net_total)

            print(
                f"CPU: {cpu:5.2f}% | RAM: {mem:8.2f} MB | Network: {net_total:8.2f} MB"
            )
    except psutil.NoSuchProcess:
        pass
    finally:
        end_time = time.time()
        total_time = end_time - start_time

        results["cpu_peak"] = max(cpu_peaks) if cpu_peaks else 0
        results["mem_peak"] = max(mem_peaks) if mem_peaks else 0
        results["net_peak"] = max(net_peaks) if net_peaks else 0
        results["duration"] = total_time
        results["cpu_avg"] = sum(cpu_peaks) / len(cpu_peaks) if cpu_peaks else 0
        results["mem_avg"] = sum(mem_peaks) / len(mem_peaks) if mem_peaks else 0
        results["net_total"] = net_peaks[-1] if net_peaks else 0

def run_and_monitor(command):
    """Run a process and monitor its performance metrics."""
    process = subprocess.Popen(command, shell=True)
    results = {}

    monitor_thread = threading.Thread(target=monitor, args=(process.pid, results))
    monitor_thread.start()

    process.wait()
    monitor_thread.join()

    print("\n=== 📊 Performance Summary ===")
    print(f"Total Time:       {results['duration']:.2f} sec")
    print(f"Peak CPU Usage:   {results['cpu_peak']:.2f}%")
    print(f"Avg CPU Usage:    {results['cpu_avg']:.2f}%")
    print(f"Peak RAM Usage:   {results['mem_peak']:.2f} MB")
    print(f"Avg RAM Usage:    {results['mem_avg']:.2f} MB")
    print(f"Total Network I/O: {results['net_total']:.2f} MB")

    # Optional speed metric (assuming total transfer size known)
    if results["net_total"] > 0:
        speed = results["net_total"] / results["duration"]
        print(f"Avg Transfer Speed: {speed:.2f} MB/s")

if __name__ == "__main__":
    # 👇 Change this command to your app’s binary
    # Example for your Rust SFTP puller:
    # cmd = "./target/release/sftp_puller_main"
    cmd = "cargo run --release"

    print(f"Starting process: {cmd}")
    run_and_monitor(cmd)



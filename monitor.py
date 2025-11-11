import psutil
import subprocess
import time
import threading

def bytes_to_mb(b):
    return b / (1024 * 1024)

def monitor(pid, results):
    """Monitor CPU, memory, and network usage for the given PID."""
    try:
        proc = psutil.Process(pid)
    except psutil.NoSuchProcess:
        return

    net_start = psutil.net_io_counters()
    start_time = time.time()

    cpu_peak = 0
    mem_peak = 0
    net_total = 0

    cpu_sum = 0
    mem_sum = 0
    count = 0

    try:
        while proc.is_running():
            # Measure CPU & memory
            cpu = proc.cpu_percent(interval=1)
            mem = proc.memory_info().rss / (1024 * 1024)  # MB

            # Update peaks and sums
            cpu_peak = max(cpu_peak, cpu)
            mem_peak = max(mem_peak, mem)
            cpu_sum += cpu
            mem_sum += mem
            count += 1

            # Network (system-wide)
            net_current = psutil.net_io_counters()
            net_sent = net_current.bytes_sent - net_start.bytes_sent
            net_recv = net_current.bytes_recv - net_start.bytes_recv
            net_total = bytes_to_mb(net_sent + net_recv)

            print(f"CPU: {cpu:5.2f}% | RAM: {mem:8.2f} MB | Network: {net_total:8.2f} MB")

    except psutil.NoSuchProcess:
        pass
    finally:
        end_time = time.time()
        total_time = end_time - start_time

        results["cpu_peak"] = cpu_peak
        results["mem_peak"] = mem_peak
        results["cpu_avg"] = cpu_sum / count if count else 0
        results["mem_avg"] = mem_sum / count if count else 0
        results["net_total"] = net_total
        results["duration"] = total_time
        results["speed"] = net_total / total_time if total_time > 0 else 0

def run_and_monitor(command):
    """Run a process and monitor its performance metrics."""
    process = subprocess.Popen(
        command,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1
    )

    results = {}
    monitor_thread = threading.Thread(target=monitor, args=(process.pid, results))
    monitor_thread.start()

    # Optional: print live stdout/stderr
    for line in process.stdout:
        print(line, end="")
    for line in process.stderr:
        print(line, end="")

    process.wait()
    monitor_thread.join()

    print("\n=== 📊 Performance Summary ===")
    print(f"Total Time:        {results['duration']:.2f} sec")
    print(f"Peak CPU Usage:    {results['cpu_peak']:.2f}%")
    print(f"Avg CPU Usage:     {results['cpu_avg']:.2f}%")
    print(f"Peak RAM Usage:    {results['mem_peak']:.2f} MB")
    print(f"Avg RAM Usage:     {results['mem_avg']:.2f} MB")
    print(f"Total Network I/O: {results['net_total']:.2f} MB")
    print(f"Avg Transfer Speed:{results['speed']:.2f} MB/s")

if __name__ == "__main__":
    # Replace this with your command
    cmd = "cargo run --release"
    print(f"Starting process: {cmd}")
    run_and_monitor(cmd)

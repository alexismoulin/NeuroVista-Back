import multiprocessing

# Number of worker processes = number of CPU cores
workers = multiprocessing.cpu_count()

# Optional: use threads within each worker
threads = 4

# Bind address
bind = "0.0.0.0:5000"

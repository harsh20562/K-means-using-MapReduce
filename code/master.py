from helper import *

"""
N = # of points
M = # of Mappers
R = # of Reduces
K = # of Centroids
iters = # of iterations for convergence
"""

class Master:
  def __init__(self, M, R, K, iters, path) -> None:
    self.points_file = path
    self.centroids_file = "Data/centroids.txt"
    self.N = count_lines(self.points_file)
    self.M = M
    self.R = R
    self.K = K
    self.iters = iters
    self.left_indices = []
    self.right_indices = []
    self.centroids = []
    self.splits = []
    self.processed_splits = []
    self.mappers_ports = [50000 + i for i in range(self.M)]
    self.reducers_ports = [60000 + i for i in range(self.R)]
    self.master_dump = "master_dump.txt"
    self.current_iter = -1
    self.active_reducers = list(range(self.R))
    self.processed_reducers = []

  def input_split(self):
    left_index = 0
    remaining_indices = self.N % self.M
    while left_index < self.N:
      right_index = min(self.N - 1, left_index + self.N // self.M - 1 + (1 if remaining_indices > 0 else 0))
      # self.left_indices.append(left_index)
      # self.right_indices.append(right_index)
      self.splits.append([left_index, right_index])
      left_index = right_index + 1
      remaining_indices -= 1

  def run_mapper(self, mapper_id):
    l, r = self.splits[mapper_id]
    if self.splits[mapper_id] in self.processed_splits:
      return
    with grpc.insecure_channel(f'localhost:{self.mappers_ports[mapper_id]}') as channel:
      stub = mapper_pb2_grpc.MapperServiceStub(channel)
      request = {
        "left_index": l,
        "right_index": r,
        "NoOfReducers": self.R
      }
      msg = f"gRPC call sent to Mapper {mapper_id} for iteration {self.current_iter} | port No. = {self.mappers_ports[mapper_id]}"
      with open(self.master_dump, 'a') as f:
        f.write(msg + '\n')
      try:
        response = stub.Map(mapper_pb2.MapRequest(**request))
        if response.status == 1:
          self.processed_splits.append([l, r])
          msg = f"Mapper {mapper_id} completed mapping for iteration {self.current_iter} | Status = {response.status} | port No. = {self.mappers_ports[mapper_id]}"
          with open(self.master_dump, 'a') as f:
            f.write(msg + '\n')
        else:
          msg = f"Mapper {mapper_id} failed mapping for iteration {self.current_iter} | Status = {response.status} | port No. = {self.mappers_ports[mapper_id]}"
          with open(self.master_dump, 'a') as f:
            f.write(msg + '\n')
      except Exception as e:
        msg = f"gRPC connection failed with Mapper {mapper_id} for iteration {self.current_iter} | port No. = {self.mappers_ports[mapper_id]}"
        with open(self.master_dump, 'a') as f:
          f.write(msg + '\n')
        return
  
  def map(self):
    while len(self.processed_splits) < self.M:
      random.shuffle(self.splits)
      pool = ThreadPool(processes=self.M)
      pool.map(self.run_mapper, range(self.M))
      pool.close()
      pool.join()
    self.processed_splits = []

  def run_reducer(self, reducer_id):
    if reducer_id in self.processed_reducers:
      return
    channel = grpc.insecure_channel(f'localhost:{self.reducers_ports[reducer_id]}')
    stub = reducer_pb2_grpc.ReducerServiceStub(channel)
    request = {
      "id": reducer_id,
      "mapper_ports": self.mappers_ports
    }
    msg = f"gRPC call sent to Reducer {reducer_id} for iteration {self.current_iter} | port No. = {self.reducers_ports[reducer_id]}"
    with open(self.master_dump, 'a') as f:
      f.write(msg + '\n')
    try:
      response = stub.Reduce(reducer_pb2.ReduceRequest(**request))
      if response.status == 1:
        self.centroids += response.centroids
        msg = f"Reducer {reducer_id} completed reducing for iteration {self.current_iter} | Status = {response.status} | port No. = {self.reducers_ports[reducer_id]}"
        with open(self.master_dump, 'a') as f:
          f.write(msg + '\n')
        self.processed_reducers.append(reducer_id)
      else:
        msg = f"Reducer {reducer_id} failed reducing for iteration {self.current_iter} | Status = {response.status} | port No. = {self.reducers_ports[reducer_id]}"
        with open(self.master_dump, 'a') as f:
          f.write(msg + '\n')
    except Exception as e:
      msg = f"gRPC connection failed with Reducer {reducer_id} for iteration {self.current_iter} | port No. = {self.reducers_ports[reducer_id]}"
      with open(self.master_dump, 'a') as f:
        f.write(msg + '\n')
      return

  def reduce(self):
    while len(self.processed_reducers) < self.R:
      random.shuffle(self.active_reducers)
      pool = ThreadPool(processes=self.R)
      pool.map(self.run_reducer, self.active_reducers)
      pool.close()
      pool.join()
    self.processed_reducers = []

  def spawn_mappers(self):
    mappers_process = []
    for mapper_id, port in enumerate(self.mappers_ports):
      process = subprocess.Popen(["python", "mapper.py", str(mapper_id), str(port), str(self.R), str(self.points_file)])
      mappers_process.append(process)
      time.sleep(1)
    return mappers_process

  def spawn_reducers(self):
    reducers_process = []
    for reducer_id, port in enumerate(self.reducers_ports):
      process = subprocess.Popen(["python", "reducer.py", str(port)])
      reducers_process.append(process)
      time.sleep(1)
    return reducers_process

  def terminate_processes(self, process_list):
    for process in process_list:
      process.terminate()

  def run_master(self):
    self.input_split()
    for iter_num in range(self.iters):
      
      # self.active_mappers = range(self.M)
      # self.active_reducers = range(self.R)

      self.current_iter = iter_num

      msg = f"Iteration {iter_num} currently running"
      with open(self.master_dump, 'a') as f:
        f.write(msg + '\n')

      mappers = self.spawn_mappers()
      self.map()

      reducers = self.spawn_reducers()
      self.reduce()

      if check_convergence(self.centroids):
        self.terminate_processes(reducers)
        self.terminate_processes(mappers)
        msg = f"Converged at iteration {iter_num}"
        with open(self.master_dump, 'a') as f:
          f.write(msg + '\n')
        return

      with open(self.centroids_file, 'w') as f:
        for centroid in self.centroids:
          f.write(f"{centroid.x},{centroid.y}\n")
      with open(self.master_dump, 'a') as f:
        f.write(f"Centroids for iteration {iter_num}:\n")
        for centroid in self.centroids:
          f.write(f"{centroid.x},{centroid.y}\n")

      self.terminate_processes(reducers)
      self.terminate_processes(mappers)
      self.centroids = []

if __name__=="__main__":
  M, R, K, iters = map(int, sys.argv[1:-1])
  path = str(sys.argv[-1])

  open("master_dump.txt", 'w').close()

  initialize_centroids(path, K)
  time.sleep(1)

  master = Master(M, R, K, iters, path)
  master.run_master()
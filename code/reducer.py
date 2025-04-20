from helper import *

"""
N = # of points
M = # of Mappers
R = # of Reduces
K = # of Centroids
iters = # of iterations for convergence
"""

"""
python reducer.py <portNo>
"""

class Reducer:
  def __init__(self, portNo):
    self.portNo = portNo
    self.partition_data = []
    self.centroids = []
  
  def run_reducer(self, mapper_port, reducer_id):
    channel = grpc.insecure_channel(f'localhost:{mapper_port}')
    stub = mapper_pb2_grpc.MapperServiceStub(channel)
    request = {
      "index": reducer_id
    }
    try:
      response = stub.GetPartition(mapper_pb2.PartitionRequest(**request))
      if response.status:
        for partitions in response.partition:
          self.partition_data += [[partitions.centroidID, partitions.x, partitions.y]]
      else:
        print(f"Error in getting partition from {mapper_port}")
    except Exception as e:
      print(f"gRPC connection failed with {mapper_port}")
      print(e)

  def shuffle_and_sort(self, reducer_id):
    self.partition_data.sort(key=lambda x: x[0])
    self.partition_data.append([-2, -2, -2])
    lastCentroid = -1
    x_sum, y_sum, num_points = 0.0, 0.0, 0
    for data in self.partition_data:
      centroid, x, y = data
      if centroid != lastCentroid:
        if lastCentroid != -1:
          self.centroids.append([lastCentroid, x_sum / num_points, y_sum / num_points])
        lastCentroid = centroid
        x_sum = 0.0
        y_sum = 0.0
        num_points = 0
      if centroid == lastCentroid:
        x_sum += x
        y_sum += y
        num_points += 1
    reducer_file = f"Data/Reducers/R{reducer_id + 1}.txt"
    with open(reducer_file, 'w') as f:
      for centroid in self.centroids:
        f.write(f"{centroid[0]},{centroid[1]},{centroid[2]}\n")

  def serve(self):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    reducer_pb2_grpc.add_ReducerServiceServicer_to_server(
      ReducerServiceHandler(self.portNo), server
    )
    server.add_insecure_port(f'[::]:{self.portNo}')
    server.start()
    server.wait_for_termination()

class ReducerServiceHandler(reducer_pb2_grpc.ReducerService, Reducer):
  def __init__(self, portNo):
    super().__init__(portNo)

  def Reduce(self, request, context):
    reducer_id = request.id
    mapper_ports = request.mapper_ports
    if not get_random_bool():
      response = {
        'status': False,
        'centroids': []
      }
      return reducer_pb2.ReduceResponse(**response)
    pool = ThreadPool(processes=len(mapper_ports))
    # pool.map(self.run_reducer, mapper_ports)
    pool.starmap(self.run_reducer, [(mapper_port, reducer_id) for mapper_port in mapper_ports])
    pool.close()
    pool.join()
    self.shuffle_and_sort(reducer_id)
    centroids = []
    for centroid in self.centroids:
      centroids.append({'id': centroid[0], 'x': centroid[1], 'y': centroid[2]})
    response = {
      'status': True,
      'centroids': centroids
    }
    return reducer_pb2.ReduceResponse(**response)

if __name__ == "__main__":
  portNo = int(sys.argv[1])
  reducer = Reducer(portNo)
  reducer.serve()
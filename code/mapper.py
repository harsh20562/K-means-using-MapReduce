from helper import *

"""
N = # of points
M = # of Mappers
R = # of Reduces
K = # of Centroids
iters = # of iterations for convergence
"""

"""
python mapper.py <mapper_id> <portNo> <R> <points_file>
"""

class Mapper:
  def __init__ (self, mapper_id, portNo, R, points_file):
    self.mapper_id = mapper_id
    self.portNo = portNo
    self.R = R

    self.point_path = points_file
    self.centroids_file = "Data/centroids.txt"
    
    self.centroids = get_centroids(self.centroids_file)
    self.cc_list = []

    self.mapper_dir = f"Data/Mappers/M{self.mapper_id + 1}"
    create_dir(self.mapper_dir)
    
    self.mapper_dump = f"{self.mapper_dir}/dump.txt"
    with open(self.mapper_dump, 'a') as f:
      f.write("")

    for r in range(1, self.R + 1):
      partition_file_path = f"partition_{r}.txt"
      with open(f"{self.mapper_dir}/{partition_file_path}", 'w') as f:
        f.write("")

  def serve(self):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    mapper_pb2_grpc.add_MapperServiceServicer_to_server(
      MapperServiceHandler(self.mapper_id, self.portNo, self.R, self.point_path), server
    )
    server.add_insecure_port(f'[::]:{self.portNo}')
    server.start()
    server.wait_for_termination()

class MapperServiceHandler(mapper_pb2_grpc.MapperService, Mapper):
  def __init__(self, mapper_id, portNo, R, points_file):
    super().__init__(mapper_id, portNo, R, points_file)

  def Map(self, request, context):
    left_index = request.left_index
    right_index = request.right_index
    noOfReducers = request.NoOfReducers

    if not get_random_bool():
      response = {
        'status': False
      }
      return mapper_pb2.MapResponse(**response)
    
    msg = f"Mapper {self.mapper_id} received request for mapping from {left_index} to {right_index}."
    with open(self.mapper_dump, 'a') as f:
      f.write(f"{msg}\n")

    points = get_points(self.point_path, left_index, right_index)
    for point in points:
      closest_centroid = -1
      dist = float("inf")
      for i, centroid in enumerate(self.centroids):
        d = get_distance(point, centroid)
        if d < dist:
          dist = d
          closest_centroid = i
      self.cc_list.append([closest_centroid, point[0], point[1]])
      partition_file_path = f"partition_{closest_centroid % self.R + 1}.txt"
      with open(f"{self.mapper_dir}/{partition_file_path}", 'a') as f:
        msg = f"{closest_centroid},{point[0]},{point[1]}"
        f.write(f"{msg}\n")
    
    response = {
      'status': True
    }
    return mapper_pb2.MapResponse(**response)

  def GetPartition(self, request, context):
    reducer_id = request.index
    data = []
    for ccs in self.cc_list:
      centroid_id, x, y = ccs
      if centroid_id % self.R == reducer_id:
        data.append({'centroidID': centroid_id, 'x': x, 'y': y})
    response = {
      'status': True,
      'partition': data
    }
    return mapper_pb2.PartitionResponse(**response)

if __name__ == '__main__': 
  mapper_id, port, R = map(int, sys.argv[1:-1])
  points_file = str(sys.argv[-1])
  mapper = Mapper(mapper_id, port, R, points_file)
  mapper.serve()
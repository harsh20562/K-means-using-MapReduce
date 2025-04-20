import sys
import grpc
import mapper_pb2 as mapper_pb2
import mapper_pb2_grpc as mapper_pb2_grpc
import reducer_pb2 as reducer_pb2
import reducer_pb2_grpc as reducer_pb2_grpc
import os
import time
import subprocess
from pathlib import Path
from multiprocessing.pool import ThreadPool
from threading import Thread
from concurrent import futures
import os
import random

def count_lines(file_name):
  with open(file_name) as f:
    return len(f.readlines())
  
def write_to_file(file_path, line):
  with open(file_path, 'w') as file:
    file.write(line + '\n')

def create_dir(dir_name: str) -> None:
  if not os.path.exists(dir_name):
    os.makedirs(dir_name)

def get_centroids(file_name):
  centroids = []
  with open(file_name) as f:
    for line in f:
      centroids.append(list(map(float, line.strip().split(','))))
  return centroids

def get_points(file_name, left_index, right_index):
  points = []
  with open(file_name) as f:
    for i, line in enumerate(f):
      if i < left_index:
        continue
      if i > right_index:
        break
      points.append(list(map(float, line.strip().split(','))))
  return points

def get_distance(point, centroid):
  return sum((p - c) ** 2 for p, c in zip(point, centroid))

def initialize_centroids(path, K):
  points = get_points(path, 0, count_lines(path) - 1)
  centroids = random.sample(points, K)
  with open("Data/centroids.txt", 'w') as f:
    for centroid in centroids:
      f.write(f"{centroid[0]},{centroid[1]}\n")
  with open("master_dump.txt", 'a') as f:
    f.write("Centroids initialized:\n")
    for centroid in centroids:
      f.write(f"{centroid[0]},{centroid[1]}\n")

def check_convergence(_new_centroids):
  old_centroids = get_centroids("Data/centroids.txt")
  new_centroids = []
  for centroid in _new_centroids:
    new_centroids.append([centroid.x, centroid.y])
  # sorting the centroids to compare
  old_centroids.sort()
  new_centroids.sort()
  # check if the centroids are equal up till 2 decimal places
  for old, new in zip(old_centroids, new_centroids):
    if round(old[0], 2) != round(new[0], 2) or round(old[1], 2) != round(new[1], 2):
      return False
  return True

def get_random_bool(p_false=0.5):
  return random.random() < p_false
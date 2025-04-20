generate_from_proto:
	python3 -m grpc_tools.protoc mapper.proto --proto_path=. --python_out=. --grpc_python_out=.
	python3 -m grpc_tools.protoc reducer.proto --proto_path=. --python_out=. --grpc_python_out=.
	python3 -m grpc_tools.protoc status.proto --proto_path=. --python_out=. --grpc_python_out=.
# imdb-streaming-application


### Run container

docker-compose build
docker-compose up

### Run tests

docker run --network host --add-host spark:127.0.0.1 -v "$(pwd)/src:/app/src" -v "$(pwd)/tests:/app/tests" chrismlittle123/imdb-streaming-app:latest python3 -m pytest tests/ -v

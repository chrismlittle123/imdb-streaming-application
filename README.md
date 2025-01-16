# imdb-streaming-application

docker-compose build
docker-compose up


docker build -t imdb-streaming-app . && docker run --rm --network host --add-host spark:127.0.0.1 -v "$(pwd)/data:/app/data" -v "$(pwd)/checkpoints:/app/checkpoints" imdb-streaming-app
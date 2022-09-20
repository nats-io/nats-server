docker buildx build --platform linux/amd64,linux/arm64/v8 -t juliuszaromskis/nats-server:latest -t juliuszaromskis/nats-server:2.8.4.3  -f ./docker/Dockerfile --push .

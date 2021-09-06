# Redis caches
docker run --rm -d --name lohpi-redis-ds -v data:/data -p 6379:6379 redis
docker run --rm -d --name lohpi-redis-ps -v data:/data -p 6380:6379 redis

# Redis caches for nodes
docker run --rm -d --name lohpi-redis-n1 -v data:/data -p 6390:6379 redis
docker run --rm -d --name lohpi-redis-n2 -v data:/data -p 6391:6379 redis
docker run --rm -d --name lohpi-redis-n3 -v data:/data -p 6392:6379 redis


# PSQL dev database
docker run -d --name lohpi-postgres-dev -e POSTGRES_PASSWORD=password! -v postgresdataa:/var/lib/postgresql/data -p 5432:5432 postgres

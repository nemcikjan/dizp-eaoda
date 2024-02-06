# dizp-eaoda

```bash
REDIS_PASSWORD=**** MAX_REALLOC=4 SIMULATION_NAME=test TEST_BED_PATH=/test-bed LOG_PATH=app.log REDIS_PORT=16379 .venv/bin/gunicorn eaoda-controller:eaoda --bind=0.0.0.0:18080 --reload
k kustomize --enable-helm manifests | kaf -
redis-cli -a $REDIS_PASSWORD KEYS "tasks:*" | xargs redis-cli -a $REDIS_PASSWORD DEL
redis-cli -a $REDIS_PASSWORD KEYS "meta:tasks:*" | xargs redis-cli -a $REDIS_PASSWORD DEL
```



for i in {0..100}; do
    curl -v -X POST localhost:18080/create -H 'content-type: application/json' -d '{"name": "test-'$i'", "priority": 4, "color": "green", "execTime": 32, "memory": 134,"cpu": 89}' 
done
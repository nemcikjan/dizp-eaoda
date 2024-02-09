

# for i in {0..100}; do
#     curl -v -X POST localhost:18080/create -H 'content-type: application/json' -d '{"name": "test-'$i'", "priority": 4, "color": "green", "execTime": 32, "memory": 134,"cpu": 89}' 
# done

#!/bin/bash

# Set the label selector for the pods you want to target
LABEL_SELECTOR="app=eapda"

# Set the command you want to run inside each pod
COMMAND="cat simulation.id"

# Get the list of pods based on the label selector
PODS=$(kubectl get pods -l $LABEL_SELECTOR -o jsonpath='{.items[*].metadata.name}')

# Iterate over the pods and run the command in each of them
for POD in $PODS
do
    echo "Running command in pod: $POD"
    kubectl exec $POD -c eaoda -- $COMMAND
done

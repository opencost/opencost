# Sample OpenCost Queries

## curl

Show total cost for all containers for the past 1 hour:

```sh
curl -G localhost:9003/allocation/compute -d window=1h
```

## kubectl-cost

Installation instructions: [kubectl cost](https://github.com/kubecost/kubectl-cost):

kubectl cost will need to be pointed at the OpenCost service. Potentially set an alias:

```sh
alias kcac='kubectl cost --service-port 9003 --service-name cost-model --kubecost-namespace cost-model --allocation-path /allocation/compute'
```

Find the cost of each namespace based on the last 5 days:

```sh
kubectl cost --service-port 9003 --service-name cost-model --kubecost-namespace cost-model --allocation-path /allocation/compute  \
    namespace \
    --historical \
    --window 5d \
    --show-cpu \
    --show-memory \
    --show-pv \
    --show-efficiency=false
```

Find the total projected monthly costs based on the last 2 hours:

```sh
kubectl cost --service-port 9003 --service-name cost-model --kubecost-namespace cost-model --allocation-path /allocation/compute  \
    namespace \
    --window 2h \
    --show-efficiency=true
```

Using the alias, show the total costs of all resources with the label app:
> Note unallocated represents resources not using the label

```sh
kcac label --historical -l app --window 5d
```

## Postman

A basic collection of OpenCost Postman queries: [opencost.postman_collection.json](./opencost.postman_collection.json)

> Note: Change the hostname in the Collection>Edit>Variables
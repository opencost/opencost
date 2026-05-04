Inference Deployment to OpenShift

# Compile for AMD64 - for our OpenShift cluster
./build-amd64.sh 

# Build docker image to push to personal github image repository
docker build --platform linux/amd64 -f Dockerfile.simple -t ghcr.io/<username>/opencost-inference:latest .

# Login to personal docker image repository on github
export CR_PAT=<your github token>
echo $CR_PAT | docker login ghcr.io -u <username --password-stdin

# Push to docker repository
docker push ghcr.io/<username>/opencost-inference:latest

# Deploy to open shift
# Login to openshift

oc login --token=<token> --server=<url>

# Assuming there is already an opencost deployment in namespace opencost
# do the following to update it
oc create secret docker-registry ghcr-secret --docker-server=ghcr.io --docker-username=<username> --docker-password <docker-token> -n opencost
oc secrets link default ghcr-secret --for=pull -n opencost
oc secrets link opencost ghcr-secret --for=pull -n opencost 2>/dev/null || true


# Create a backup of the current deployment
oc get deployment opencost -n opencost -o yaml > opencost-deployment-backup-$(date +%Y%m%d-%H%M%S).yaml

oc set image deployment/opencost \
  opencost=ghcr.io/<username>/opencost-inference:latest -n opencost

oc patch deployment opencost -n opencost -p '{"spec":{"template":{"spec":{"containers":[{"name":"opencost","imagePullPolicy":"Always"}]}}}}'

oc set env deployment/opencost -n opencost \
  INFERENCE_COST_ENABLED=true \
  INFERENCE_COST_COLLECTION_INTERVAL=60 \
  PROMETHEUS_SERVER_ENDPOINT=http://prometheus-server:9090


# After initial deployment if changes are made to the image
# compile again, create and push docker image again, and then
oc rollout restart deployment/opencost -n opencost

# To view the inference metrics
oc port-forward -n opencost svc/opencost 9003:9003

curl http://localhost:9003/metrics | grep opencost_inference_cost_per_million_tokens

	Xferd  Average Speed   Time    Time     Time  Current
	                                 Dload  Upload   Total   Spent    Left  Speed
	100 4367k    0 4367k    0     0   195k      0 --:--:--  0:00:22 --:--:-- 64030# HELP opencost_inference_cost_per_million_tokens Cost per 1 million tokens processed (input + output) for a specific model in a specific namespace
	# TYPE opencost_inference_cost_per_million_tokens gauge
	opencost_inference_cost_per_million_tokens{model_name="MiniMaxAI/MiniMax-M2.7",model_version="unknown",namespace="llm-d-pic"} 0.004344126665201276
	opencost_inference_cost_per_million_tokens{model_name="Qwen/Qwen3-32B",model_version="unknown",namespace="aruocco"} 0
	opencost_inference_cost_per_million_tokens{model_name="Qwen/Qwen3-32B",model_version="unknown",namespace="dpikus-precise-new"} 0
	opencost_inference_cost_per_million_tokens{model_name="Qwen/Qwen3-32B",model_version="unknown",namespace="llm-d-precise"} 36.55271644458403
	opencost_inference_cost_per_million_tokens{model_name="Qwen/Qwen3-32B",model_version="unknown",namespace="rachelt-benchmark"} 1000.5508926904979
	opencost_inference_cost_per_million_tokens{model_name="random",model_version="unknown",namespace="dpikus-sim"} 0
	100 4582k    0 4582k    0     0   178k      0 --:--:--  0:00:25 --:--:-- 66948
	
	
curl http://localhost:9003/metrics | grep opencost_inference_total_cost             
	  % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current
	                                 Dload  Upload   Total   Spent    Left  Speed
	100 4087k    0 4087k    0     0   304k      0 --:--:--  0:00:13 --:--:--  347k# HELP opencost_inference_total_cost Total infrastructure cost attributed to inference for a specific model in a specific namespace
	# TYPE opencost_inference_total_cost gauge
	opencost_inference_total_cost{model_name="MiniMaxAI/MiniMax-M2.7",model_version="unknown",namespace="llm-d-pic"} 0.018048
	opencost_inference_total_cost{model_name="Qwen/Qwen3-32B",model_version="unknown",namespace="aruocco"} 0.001128
	opencost_inference_total_cost{model_name="Qwen/Qwen3-32B",model_version="unknown",namespace="dpikus-precise-new"} 0
	opencost_inference_total_cost{model_name="Qwen/Qwen3-32B",model_version="unknown",namespace="llm-d-precise"} 0.018048
	opencost_inference_total_cost{model_name="Qwen/Qwen3-32B",model_version="unknown",namespace="rachelt-benchmark"} 0.018048
	opencost_inference_total_cost{model_name="random",model_version="unknown",namespace="dpikus-sim"} 0
	100 4599k    0 4599k    0     0   303k      0 --:--:--  0:00:15 --:--:--  337k

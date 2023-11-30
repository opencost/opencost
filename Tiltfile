load('ext://helm_resource', 'helm_resource', 'helm_repo')
load('ext://restart_process', 'docker_build_with_restart')

# WARNING: this allows any k8s context for deployment
#allow_k8s_contexts(k8s_context())
# To allow a specific context for deployment:
# allow_k8s_contexts('kubectl-context')
# See https://docs.tilt.dev/api.html#api.allow_k8s_contexts for default allowed contexts

config.define_string('arch', args=False, usage='amd64')
config.define_string('docker-repo', args=False, usage='')
cfg = config.parse()

arch = cfg.get('arch')

docker_platform = "linux/amd64"
go_arch = "amd64"
if arch == "arm64":
    docker_platform = "linux/aarch64"
    go_arch = "arm64"

docker_repo = cfg.get('docker-repo')
if docker_repo == None:
    docker_repo = ''
else:
    docker_repo = docker_repo + "/"

# Build and update opencost back end binary when code changes
local_resource(
    name='build-costmodel',
    dir='.',
    cmd='CGO_ENABLED=0 GOOS=linux GOARCH='+go_arch+' go build -o ./cmd/costmodel/costmodel-tilt ./cmd/costmodel/main.go',
    deps=[
        './cmd/costmodel/main.go',
        './pkg',
    ],
    allow_parallel=True,
    resource_deps=['build-go-mod-download'],
)

# Build back end docker container
# If the binary is updated, update the running container and restart binary in dlv
docker_build_with_restart(
    ref=docker_repo+'opencost-costmodel',
    context='.',
    # remove --continue flag to make dlv wait until debugger is attached to start
    entrypoint='/go/bin/dlv exec --listen=:40000 --api-version=2 --headless=true --accept-multiclient --log --continue /app/main',
    dockerfile='Dockerfile.debug',
    platform=docker_platform,

    build_args={'binary_path':'./cmd/costmodel/costmodel-tilt'},
    only=[
        'cmd/costmodel/costmodel-tilt',
        'configs',
    ],
    live_update=[
       sync('./cmd/costmodel/costmodel-tilt', '/app/main'),
    ],
)

# npm install if package.json changes
local_resource(
    name='build-npm-install',
    dir='./ui',
    cmd='npm install',
    deps=[
        './ui/package.json',
    ],
    allow_parallel=True,
)

# Build FE locally when code changes
local_resource(
    name='build-ui',
    dir='./ui',
    cmd='npx parcel build src/index.html',
    deps=[
        './ui/src',
        './ui/package.json',
    ],
    allow_parallel=True,
    resource_deps=['build-npm-install'],
)

# update container when relevant files change
docker_build(
    ref=docker_repo+'opencost-ui',
    context='./ui',
    dockerfile='./ui/Dockerfile.cross',
    only=[
        'dist',
        'nginx.conf',
        'default.nginx.conf',
        'docker-entrypoint.sh',
    ],
    live_update=[
       sync('./ui/dist', '/var/www'),
    ],
)

# build yaml for deployment to k8s
yaml = helm(
    '../opencost-helm-chart/charts/opencost',
    name='opencost',
    values=['./tilt-values.yaml'],
    # configuring opencost to also use the kubecost prometheus server below
    set=[
        'opencost.ui.image.fullImageName='+docker_repo+'opencost-ui',
        'opencost.exporter.image.fullImageName='+docker_repo+'opencost-costmodel',
        'opencost.prometheus.internal.namespaceName='+k8s_namespace(),
    ]
)
k8s_yaml(yaml) # put resulting yaml into k8s
k8s_resource(workload='opencost', port_forwards=['9003:9003','9090:9090','40000:40000'])

helm_resource(
    name='prometheus',
    chart='prometheus-community/prometheus')
k8s_resource(workload='prometheus', port_forwards=['9080:9090'])

local_resource(
    name='costmodel-test',
    dir='.',
    cmd='go test ./...',
    deps=[
        './pkg',
    ],
    allow_parallel=True,
    resource_deps=['opencost'], # run tests after build to speed up deployment
)
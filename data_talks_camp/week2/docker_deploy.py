from prefect.deployments import Deployment
from prefect.infrastructure.container import DockerContainer
from etl_gcp import diff_months

docker_block = DockerContainer.load("nyc-docker")

docker_dep = Deployment.build_from_flow(
    flow=diff_months,
    name='docker-flow',
    infrastructure=docker_block
)

if __name__ =='__main__':
    docker_dep.apply()
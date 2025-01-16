from kubernetes import client, watch, config

WORKER_CONTAINER_IMAGE = "docker.io/aitorarjona/rapidask-k8s:dev"
# WORKER_CONTAINER_IMAGE = "registry.k8s.io/e2e-test-images/agnhost:2.39"


if __name__ == "__main__":
    config.load_kube_config()

    cluster_name = "test"
    cpu = 0.125
    memory = "256M"

    label = f"rapidask-{cluster_name}"
    metadata = client.V1ObjectMeta(name=label, labels={"app": label})

    worker_container = client.V1Container(
        name="worker",
        image=WORKER_CONTAINER_IMAGE,
        ports=[client.V1ContainerPort(container_port=8888)],
        resources=client.V1ResourceRequirements(
            limits={
                "cpu": cpu,
                "memory": memory,
            },
            requests={
                "cpu": cpu,
                "memory": memory,
            },
        ),
        image_pull_policy="Always",
        command=["python"],
        args=["-m", "distributed.burst.deploy.worker"]
    )

    template_metadata = client.V1ObjectMeta(labels={"app": label})
    template_spec = client.V1PodSpec(containers=[worker_container])
    pod_template = client.V1PodTemplateSpec(
        metadata=template_metadata,
        spec=template_spec
    )

    replicaset_spec = client.V1ReplicaSetSpec(
        replicas=2,
        selector={"matchLabels": {"app": label}},
        template=pod_template
    )
    replicaset = client.V1ReplicaSet(
        api_version="apps/v1",
        kind="ReplicaSet",
        metadata=metadata,
        spec=replicaset_spec
    )

    apps_api = client.AppsV1Api()
    apps_api.create_namespaced_replica_set(namespace="default", body=replicaset)

    w = watch.Watch()
    core_api = client.CoreV1Api()

    for event in w.stream(core_api.list_namespaced_pod, namespace="default", label_selector=f"app={label}"):
        pod = event["object"]
        pod_name = pod.metadata.name
        pod_ip = pod.status.pod_ip
        print(f"📦 Pod: {pod_name}, IP: {pod_ip}")
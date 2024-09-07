from kubernetes import client
from kubernetes.client.rest import ApiException


def run_spark_app(api_client: client.ApiClient, app_config: dict):
    """Function for running spark application on Kubernetes using
       config.

    Args:
        api_client (client.ApiClient): Kubernetes API client
        app_config (dict): Kubernetes manifest as dict

    Returns:
        Kubernetes API response
    """
    custom_obj_api = client.CustomObjectsApi(api_client)
    try:
        response = custom_obj_api.create_namespaced_custom_object(
            body=app_config,
            group='sparkoperator.k8s.io',
            version='v1beta2',
            plural='sparkapplications',
            namespace='spark',
        )
        return response
    except ApiException as e:
        raise e


def delete_spark_app(api_client: client.ApiClient, app_name: str):
    """Function for deleting spark application on Kubernetes using
       config.

    Args:
        api_client (client.ApiClient): Kubernetes API client
        app_name (str): application name to delete
    """
    custom_obj_api = client.CustomObjectsApi(api_client)
    custom_obj_api.delete_namespaced_custom_object(
        group='sparkoperator.k8s.io',
        version='v1beta2',
        name=app_name,
        namespace='spark',
        plural='sparkapplications',
        body=client.V1DeleteOptions(),
    )


def stream_pod_logs(core_api_client: client.CoreV1Api, pod_name: str, namespace: str):
    """Generator for streaming Kubernetes running pod logs.

    Args:
        api_client (client.CoreV1Api): Kubernetes API client
        pod_name (str): Kubernetes pod name to stream
        namespace (str): Kubernetes namespace there pod is running
    """
    for line in core_api_client.read_namespaced_pod_log(
        name=pod_name,
        namespace=namespace,
        follow=True,
        _preload_content=False,
    ).stream():
        yield line.decode()

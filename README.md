# spark8s

Sparkinetes is a python library for manipulating manifests of spark applications that run
in a k8s-cluster and managing those apps.

It provides methods for:
- creating manifests with API;
- managing Apache Spark applications in a kubernetes cluster with a [spark-on-k8s-operator](https://github.com/GoogleCloudPlatform/spark-on-k8s-operator) (run apps using manifest, delete apps, monitoring).

## Basic usage

```python
app_name = 'test-app'

# create manifest using static method
app = AppManifest.load_default_manifest(app_name)

app.set_driver_spec(1, '1000m', 2)
app.set_executor_spec(1, '1000m', 2, 4)
app.set_application_file('local:////somefile.py')

manifest = app.get_manifest()

# create kubernetes api client
api = client.ApiClient()
core_v1_api = client.CoreV1Api()
pod_name = f'{app_name}-driver'

# run app, stream logs, delete app and allocated resources
run_spark_app(api, manifest)

for line in stream_pod_logs(core_v1_api, pod_name, namespace):
    log.info(line)

delete_spark_app(api, app_name)
```

## Advanced functions

Besides basic parameters like app name, app file and pods specs, you can also pass environment variables and Kubernetes secrets, or load manifest from existing yaml file to manipulate with it.

```python
app = AppManifest.load_from_file('test-app', 'path/to/file.yaml')

app.add_env_variables({'VAR_NAME': 'VAR_VALUE'})
app.add_kube_secrets({'SECRET': {'name': 's3-secret', 'key': 'S3_SECRET_KEY'}})

manifest = app.get_manifest()
```

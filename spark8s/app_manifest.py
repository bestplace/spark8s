import yaml
from schema import And, Optional, Or, Regex, Schema


class AppManifest:
    def __init__(self, name: str):
        self._name = name
        self._manifest = None

    @staticmethod
    def load_default_manifest(name: str) -> 'AppManifest':
        """Method for creating AppManifest object and load default manifest

        Args:
            name (str): kubernetes application name

        Returns:
            AppManifest
        """
        manifest = {
            'apiVersion': 'sparkoperator.k8s.io/v1beta2',
            'kind': 'SparkApplication',
            'metadata': {'name': name, 'namespace': 'spark'},
            'spec': {
                'type': 'Python',
                'pythonVersion': '3',
                'mode': 'cluster',
                'image': '',
                'imagePullPolicy': 'Always',
                'sparkVersion': '3.5.0',
                'restartPolicy': {'type': 'Never'},
                'sparkConf': {
                    'spark.local.dir': '/mnt/spark/tmp',
                    'spark.shuffle.compress': 'true',
                    'spark.shuffle.spill.compress': 'true',
                    'spark.sql.shuffle.partitions': '10',
                },
                'volumes': [
                    {
                        'name': 'spark-data',
                        'persistentVolumeClaim': {'claimName': 'spark-data'},
                    },
                ],
                'driver': {
                    'labels': {'version': '3.5.0'},
                    'serviceAccount': 'spark-driver',
                    'volumeMounts': [{'name': 'spark-data', 'mountPath': '/mnt/spark'}],
                },
                'executor': {
                    'labels': {'version': '3.5.0'},
                    'volumeMounts': [{'name': 'spark-data', 'mountPath': '/mnt/spark'}],
                },
            },
        }

        app = AppManifest(name)
        app._manifest = manifest
        return app

    @staticmethod
    def load_from_file(name: str, file_path: str) -> 'AppManifest':
        """Method for creating AppManifest object and load manifest from file

        Args:
            name (str): kubernetes application name
            file_path (str): path to yaml file with k8s-manifest

        Returns:
            AppManifest
        """
        app = AppManifest(name)
        with open(file_path, 'r') as file:
            application_config = yaml.safe_load(file)
            app._manifest = application_config

            app.set_application_name(name)

        return app

    def get_manifest(self):
        self.validate_manifest()
        return self._manifest

    def set_application_args(self):
        raise NotImplementedError

    def set_application_file(self, app_file_path: str):
        self._manifest['spec']['mainApplicationFile'] = app_file_path

    def set_application_name(self, name: str):
        self._name = name
        self._manifest['metadata']['name'] = name

    def set_driver_spec(self, cores: int, core_limit: str, memory: int):
        specs = {
            'cores': cores,
            'coreLimit': core_limit,
            'memory': f'{memory}g',
        }
        try:
            self._manifest['spec']['driver'].update(specs)
        except KeyError:
            raise AttributeError('Manifest corrupted: no spec and driver section.')

    def set_executor_spec(
        self, cores: int, core_limit: str, memory: int, instances: int
    ):
        specs = {
            'cores': cores,
            'coreLimit': core_limit,
            'instances': instances,
            'memory': f'{memory}g',
        }
        try:
            self._manifest['spec']['executor'].update(specs)
        except KeyError:
            raise AttributeError('Manifest corrupted: no spec and executor section.')

    def add_env_variables(self, variables: dict):
        envs = [{'name': key, 'value': value} for key, value in variables.items()]
        try:
            if self._manifest['spec']['driver'].get('env'):
                self._manifest['spec']['driver']['env'].extend(envs)
            else:
                self._manifest['spec']['driver']['env'] = envs
        except KeyError:
            raise AttributeError('Manifest corrupted: no spec and driver section.')

    def add_kube_secrets(self, variables: dict):
        envs = [
            {'name': key, 'valueFrom': {'secretKeyRef': value}}
            for key, value in variables.items()
        ]
        try:
            if self._manifest['spec']['driver'].get('env'):
                self._manifest['spec']['driver']['env'].extend(envs)
            else:
                self._manifest['spec']['driver']['env'] = envs
        except KeyError:
            raise AttributeError('Manifest corrupted: no spec and driver section.')

    def validate_manifest(self):
        manifest_schema = Schema(
            {
                'apiVersion': 'sparkoperator.k8s.io/v1beta2',
                'kind': 'SparkApplication',
                'metadata': {'name': str, 'namespace': 'spark'},
                'spec': {
                    'type': 'Python',
                    'pythonVersion': '3',
                    'mode': 'cluster',
                    'image': '',
                    'imagePullPolicy': 'Always',
                    'mainApplicationFile': str,
                    'sparkVersion': Regex('3.[0-6].[0-9]'),
                    'restartPolicy': {'type': 'Never'},
                    Optional('sparkConf'): {Optional(str): str},
                    'volumes': [
                        {
                            'name': 'spark-data',
                            'persistentVolumeClaim': {'claimName': 'spark-data'},
                        },
                    ],
                    'driver': {
                        'cores': And(Or(float, int), lambda x: x > 0),
                        'coreLimit': Regex('^\d{3,6}m$'),
                        'memory': Or(Regex('^\d{1,3}g$'), Regex('^\d{1,3}.\dg$')),
                        'labels': {'version': Regex('3.[0-9].[0-9]')},
                        'serviceAccount': 'spark-driver',
                        'volumeMounts': [
                            {'name': 'spark-data', 'mountPath': '/mnt/spark'}
                        ],
                        Optional('env'): [
                            {
                                'name': str,
                                Optional('valueFrom'): {
                                    'secretKeyRef': {'name': str, 'key': str}
                                },
                                Optional('value'): str,
                            }
                        ],
                    },
                    'executor': {
                        'cores': And(Or(float, int), lambda x: x > 0),
                        'coreLimit': Regex('^\d{3,6}m$'),
                        'memory': Or(Regex('^\d{1,3}g$'), Regex('^\d{1,3}.\dg$')),
                        'instances': And(int, lambda x: x > 0),
                        'labels': {'version': Regex('3.[0-9].[0-9]')},
                        'volumeMounts': [
                            {'name': 'spark-data', 'mountPath': '/mnt/spark'}
                        ],
                    },
                },
            }
        )

        manifest_schema.validate(self._manifest)

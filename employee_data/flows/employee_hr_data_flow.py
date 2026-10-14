from datetime import datetime
from airflow.sdk import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
#from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor TEMP
from airflow.utils.task_group import TaskGroup
from botocore.client import Config

from airflow.providers.trino.hooks.trino import TrinoHook
from airflow.sdk import Variable

import json
import os
import re
import sys
import boto3
import yaml
from airflow.utils.email import send_email

s3_workflows_config_path_prefix = "employee_data/config/employee_hr_data.json"
_DAG_PACKAGE_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_FLOWS_DIR = os.path.dirname(os.path.abspath(__file__))
if _FLOWS_DIR not in sys.path:
    sys.path.insert(0, _FLOWS_DIR)
from metastore_uris import resolve_metastore_uri, resolve_trino_connection_id

dag_default_args = Variable.get("AIRFLOW_DAG_DEFAULT_ARGS", deserialize_json=True)


class ReadAirflowVariables:

    @staticmethod
    def get_global_vars():
        global_vars = Variable.get("GLOBALS", deserialize_json=True)

        global_vars["trino_qb_details"] = {
            "transformed": {
                "trino_connection": TrinoHook(global_vars["trino_airflow_connections"].get('transformed', None)),
                "querybook_api_jobs": global_vars["querybook_api_jobs"],
                "querybook_api_metastores": global_vars["querybook_api_metastores"],
                "querybook_api_key": global_vars["querybook_api_key"],
                "querybook_metastore_name": "transformed",
                "bucket": "transformed"
            },
            "raw": {
                "trino_connection": TrinoHook(global_vars["trino_airflow_connections"].get('raw', None)),
                "querybook_api_jobs": global_vars["querybook_api_jobs"],
                "querybook_api_metastores": global_vars["querybook_api_metastores"],
                "querybook_api_key": global_vars["querybook_api_key"],
                "querybook_metastore_name": "raw",
                "bucket": "raw",
                "hive_raw_metastore_uri": global_vars["hive_raw_metastore_uri"]
            },
            "dataproducts": {
                "trino_connection": TrinoHook(
                    resolve_trino_connection_id(global_vars, "dataproducts")
                ),
                "querybook_api_jobs": global_vars["querybook_api_jobs"],
                "querybook_api_metastores": global_vars["querybook_api_metastores"],
                "querybook_api_key": global_vars["querybook_api_key"],
                "querybook_metastore_name": "dataproducts",
                "bucket": "dataproducts"
            }
        }
        return global_vars

    @staticmethod
    def _load_airflow_pipelines():
        raw_pipelines = Variable.get("AIRFLOW_PIPELINES")
        try:
            return json.loads(raw_pipelines)
        except json.JSONDecodeError as exc:
            repaired = re.sub(r",(\s*[}\]])", r"\1", raw_pipelines)
            try:
                parsed = json.loads(repaired)
                print(
                    "WARNING: AIRFLOW_PIPELINES had invalid JSON (%s); "
                    "parsed after removing trailing commas. "
                    "Fix Admin → Variables to valid JSON."
                    % exc
                )
                return parsed
            except json.JSONDecodeError:
                print(
                    "WARNING: AIRFLOW_PIPELINES is invalid JSON (%s). "
                    "Using airflow-dag-details fallback for this DAG."
                    % exc
                )
                return {}

    @staticmethod
    def get_airflow_pipelines_vars(airflow_dag_details_from_config):
        dag_id = airflow_dag_details_from_config["dag_id"]
        fallback_pipeline_vars = {
            "catalog": airflow_dag_details_from_config.get("catalog", "raw"),
            "schedule": airflow_dag_details_from_config.get(
                "schedule", ""
            ),
            "tags": airflow_dag_details_from_config.get("tags") or [],
        }
        airflow_pipelines = ReadAirflowVariables._load_airflow_pipelines()
        if dag_id not in airflow_pipelines:
            print(
                'WARNING: AIRFLOW_PIPELINES is missing key "%s". '
                "Using airflow-dag-details fallback." % dag_id
            )
            airflow_pipeline_vars = fallback_pipeline_vars
        else:
            airflow_pipeline_vars = dict(airflow_pipelines[dag_id])
            for key, value in fallback_pipeline_vars.items():
                if value in ("", [], None):
                    continue
                airflow_pipeline_vars.setdefault(key, value)

        schedule = airflow_pipeline_vars.get("schedule", None)
        if schedule == "":
            airflow_pipeline_vars["schedule"] = None
        else:
            airflow_pipeline_vars["schedule"] = schedule

        start_date = "2023-05-06 00:00:00"
        if start_date == "" or start_date is None:
            now = datetime.now()
            airflow_pipeline_vars["start_date"] = datetime.strptime(
                now.strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S"
            )
        else:
            airflow_pipeline_vars["start_date"] = datetime.strptime(
                str(start_date), "%Y-%m-%d %H:%M:%S"
            )

        return airflow_pipeline_vars


class ReadPipelineConfig:

    @staticmethod
    def read_config_from_local(s3_workflows_config_path_prefix):
        """Fallback when MinIO object is missing — use file next to the DAG package."""
        candidates = [
            os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                os.path.basename(s3_workflows_config_path_prefix),
            ),
            os.path.join(
                _DAG_PACKAGE_ROOT,
                "config",
                os.path.basename(s3_workflows_config_path_prefix),
            ),
            os.path.join(
                os.path.dirname(_DAG_PACKAGE_ROOT), s3_workflows_config_path_prefix
            ),
        ]
        for local_path in candidates:
            if os.path.isfile(local_path):
                with open(local_path, "r", encoding="utf-8") as fh:
                    return json.load(fh)
        raise FileNotFoundError(
            "Local config not found. Tried: {}".format(", ".join(candidates))
        )

    @staticmethod
    def read_config_from_s3(s3_workflows_config_path_prefix, global_vars):
        s3_resource = boto3.resource(
            's3',
            endpoint_url=global_vars["s3_host"],
            aws_access_key_id=global_vars["s3_access_key"],
            aws_secret_access_key=global_vars["s3_secret_access_key"],
            config=Config(signature_version=global_vars["s3_signature_version"]),
            region_name=global_vars["s3_region"]
        )
        try:
            response = s3_resource.Object(
                global_vars["buckets"]["workflows"], s3_workflows_config_path_prefix
            )
            text = response.get()['Body'].read().decode('utf-8')
            return json.loads(text)
        except Exception as e:
            try:
                configurations = ReadPipelineConfig.read_config_from_local(
                    s3_workflows_config_path_prefix
                )
                print(
                    "WARNING: S3 config missing ({}), using local fallback".format(
                        s3_workflows_config_path_prefix
                    )
                )
                return configurations
            except Exception as local_err:
                raise Exception(
                    "Exception occurred while reading Config File: %s, "
                    "S3 Error:%s, Local Error:%s "
                    % (s3_workflows_config_path_prefix, str(e), str(local_err))
                )

    @staticmethod
    def get_airflow_dag_details_from_config(configs):
        airflow_dag_details = dict(
            configs.get("airflow-dag-details") or configs["aiflow-dag-details"]
        )
        return airflow_dag_details


class KubernetesSparkSetup:

    @staticmethod
    def handle_min_resources(spark_kubernetes_job_args):
        driver_cores = spark_kubernetes_job_args["driver_cores"]
        executor_cores = spark_kubernetes_job_args["executor_cores"]
        driver_memory = str(spark_kubernetes_job_args["driver_memory"])
        executor_memory = str(spark_kubernetes_job_args["executor_memory"])
        executor_instances = spark_kubernetes_job_args["executor_instances"]

        if str(driver_memory).__contains__("g"):
            d_memory = int(driver_memory.split("g")[0]) * 1024
        elif str(driver_memory).__contains__("m"):
            d_memory = int(driver_memory.split("m")[0])
        else:
            raise Exception("Provide driver_memory in as suffix as 'g' or 'm'")

        if str(executor_memory).__contains__("g"):
            e_memory = int(executor_memory.split("g")[0]) * 1024
        elif str(executor_memory).__contains__("m"):
            e_memory = int(executor_memory.split("m")[0])
        else:
            raise Exception("Provide executor_memory in as suffix as 'g' or 'm'")

        min_cores = str(driver_cores + (executor_instances * executor_cores))
        min_memory = str(d_memory + (executor_instances * e_memory)) + "m"

        return min_cores, min_memory

    @staticmethod
    def create_cluster_details_spark_args(global_vars, airflow_pipeline_vars):
        configured_catalog = airflow_pipeline_vars["catalog"]
        cluster_details_spark_args = {
            "s3_credentials": {
                "s3_host": global_vars["s3_host"],
                "s3_access_key": global_vars["s3_access_key"],
                "s3_secret_access_key": global_vars["s3_secret_access_key"],
                "s3_region": global_vars["s3_region"],
                "s3_signature_version": global_vars["s3_signature_version"]
            },
            "catalog": configured_catalog,
            "catalog_minio_bucket":
                global_vars["trino_qb_details"][configured_catalog]["bucket"]
        }
        return cluster_details_spark_args

    @staticmethod
    def build_k8s_pyspark_configuration(spark_app_name, spark_kubernetes_job_args, spark_job_args,
                                        global_vars, airflow_pipeline_vars, job_arguments=[]):
        spark_language = spark_kubernetes_job_args.get('spark_language', 'python').lower()

        cluster_details_spark_args = KubernetesSparkSetup.create_cluster_details_spark_args(
            global_vars, airflow_pipeline_vars
        )
        spark_job_args = dict(spark_job_args)
        spark_job_args["catalog_minio_bucket"] = cluster_details_spark_args["catalog_minio_bucket"]
        hadoop_custom_arguments = {
            "spark_job_args": spark_job_args,
            "cluster_details_spark_args": cluster_details_spark_args
        }

        k8s_configuration = {
            "apiVersion": "sparkoperator.k8s.io/v1beta2",
            "kind": "SparkApplication",
            "metadata": {
                "name": spark_app_name,
                "namespace": global_vars["k8s_namespace"],
                "labels": {
                    "app": "spark-jobs"
                }
            },
            "spec": {
                "type": "Scala" if spark_language == 'scala' else 'Python',
                "mode": "cluster",
                "image": global_vars["spark_image"] if spark_language == 'scala' else global_vars["pyspark_image"],
                "imagePullPolicy": "Always",
                "mainApplicationFile": spark_kubernetes_job_args["pyspark_job_main_file_s3_path"],
                "arguments": job_arguments,
                "sparkVersion": global_vars["spark_version"],
                "batchScheduler": "volcano",
                "batchSchedulerOptions": {
                    "priorityClassName": spark_kubernetes_job_args.get('volcano_job_priority', 'routine'),
                    "resources": {
                        "cpu": KubernetesSparkSetup.handle_min_resources(spark_kubernetes_job_args)[0],
                        "memory": KubernetesSparkSetup.handle_min_resources(spark_kubernetes_job_args)[1]
                    }
                },
                "restartPolicy": {
                    "type": "Never"
                },
                "timeToLiveSeconds": global_vars["k8s_pod_ttl"],
                "volumes": [
                    {
                        "name": global_vars["volume_details"]["volumes_name"],
                        "persistentVolumeClaim": {
                            "claimName": global_vars["volume_details"]["volumes_claim_name"]
                        }
                    }
                ],
                "driver": {
                    "cores": spark_kubernetes_job_args["driver_cores"],
                    "memory": spark_kubernetes_job_args["driver_memory"],
                    "labels": {
                        "version": "3.1.1"
                    },
                    "serviceAccount": global_vars["k8s_service_account"],
                    "volumeMounts": [
                        {
                            "mountPath": global_vars["volume_details"]["volume_mounts_mount_path"],
                            "name": global_vars["volume_details"]["volume_mounts_name"]
                        }
                    ]
                },
                "executor": {
                    "instances": spark_kubernetes_job_args["executor_instances"],
                    "cores": spark_kubernetes_job_args["executor_cores"],
                    "memory": spark_kubernetes_job_args["executor_memory"],
                    "labels": {
                        "version": "3.1.1"
                    },
                    "volumeMounts": [
                        {
                            "mountPath": global_vars["volume_details"]["volume_mounts_mount_path"],
                            "name": global_vars["volume_details"]["volume_mounts_name"]
                        }
                    ],
                },
                "sparkConf": {
                    "spark.hadoop.custom.arguments": json.dumps(hadoop_custom_arguments),
                    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
                    "spark.sql.catalog.spark_catalog": "org.apache.iceberg.spark.SparkSessionCatalog",
                    "spark.sql.catalog.spark_catalog.type": "hive",
                    "spark.sql.catalog.raw": "org.apache.iceberg.spark.SparkCatalog",
                    "spark.sql.catalog.raw.type": "hive",
                    "spark.sql.catalog.raw.uri": resolve_metastore_uri(global_vars, "raw"),
                    "spark.sql.catalog.raw.warehouse": "s3a://%s/"
                    % cluster_details_spark_args["catalog_minio_bucket"],
                    "spark.sql.catalog.transformed": "org.apache.iceberg.spark.SparkCatalog",
                    "spark.sql.catalog.transformed.uri": resolve_metastore_uri(
                        global_vars, "transformed"
                    ),
                    "spark.sql.catalog.dataproducts": "org.apache.iceberg.spark.SparkCatalog",
                    "spark.sql.catalog.dataproducts.type": "hive",
                    "spark.sql.catalog.dataproducts.uri": resolve_metastore_uri(
                        global_vars, "dataproducts"
                    ),
                    "spark.sql.catalog.dataproducts.warehouse": "s3a://dataproducts/",
                    "spark.sql.iceberg.handle-timestamp-without-timezone": "true",
                    "spark.eventLog.enabled": "true",
                    "spark.history.fs.logDirectory": global_vars["s3_spark_logging_path"],
                    "spark.eventLog.dir": global_vars["s3_spark_logging_path"],
                    "spark.hadoop.fs.s3a.endpoint": global_vars["s3_host"],
                    "spark.hadoop.fs.s3a.access.key": global_vars["s3_access_key"],
                    "spark.hadoop.fs.s3a.secret.key": global_vars["s3_secret_access_key"],
                    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
                    "spark.hadoop.fs.s3a.path.style.access": "true",
                    "spark.sql.parquet.fs.optimized.committer.optimization-enabled": "true"
                }
            }
        }
        extra_classpath = spark_kubernetes_job_args.get("spark_dependencies_libraries")
        if extra_classpath:
            k8s_configuration["spec"]["sparkConf"]["spark.executor.extraClassPath"] = extra_classpath
            k8s_configuration["spec"]["sparkConf"]["spark.driver.extraClassPath"] = extra_classpath
        return k8s_configuration


class BuildDagTaskGroup:

    @staticmethod
    def build_spark_details(dag, airflow_task_details, task_group_key, task_details_key,
                            global_vars, airflow_pipeline_vars, common_arguments):

        airflow_tasks_list = []
        spark_kubernetes_args = airflow_task_details[task_group_key][task_details_key][
            "spark_kubernetes_args"]
        spark_job_args = airflow_task_details[task_group_key][task_details_key]["spark_job_args"]

        for common_args_key in common_arguments.keys():
            if common_args_key not in spark_job_args.keys():
                spark_job_args[common_args_key] = common_arguments[common_args_key]

        # kubernetes_spark_monitoring_task = "{{ task_instance.xcom_pull(task_ids='%s')['metadata']['name'] }}"  TEMP

        # Unique per attempt: leftover SparkApplication CRDs cause 409 AlreadyExists on retry.
        spark_app_name = "{}-{}-{}-{}".format(
            task_group_key,
            "{{ ts_nodash | lower }}",
            "{{ ti.try_number }}",
            "{{ macros.uuid.uuid4().hex[:4] }}",
        )

        spark_submit_job = SparkKubernetesOperator(
            task_id="spark-submit",
            namespace=global_vars["k8s_namespace"],
            kubernetes_conn_id=global_vars["k8s_connection_id"],
            application_file=yaml.dump(
                KubernetesSparkSetup.build_k8s_pyspark_configuration(
                    spark_app_name,
                    spark_kubernetes_args,
                    spark_job_args,
                    global_vars,
                    airflow_pipeline_vars,
                    job_arguments=["{}".format(task_group_key)]
                )
            ),
            do_xcom_push=False,#TEMP(WAS TRUE)
            dag=dag,
        )

        # TEMP: SparkKubernetesSensor removed - operator now waits for the job itself
        # spark_monitor_job = SparkKubernetesSensor(
        #     task_id="spark-monitor",
        #     namespace=global_vars["k8s_namespace"],
        #     kubernetes_conn_id=global_vars["k8s_connection_id"],
        #     application_name=kubernetes_spark_monitoring_task
        #     % "{}.spark-submit".format(task_group_key),
        #     dag=dag,
        #     attach_log=True
        # )

        airflow_tasks_list.append(spark_submit_job)
        # airflow_tasks_list.append(spark_monitor_job)

        return airflow_tasks_list

    @staticmethod
    def build_trino_details(airflow_task_details, task_group_key, task_details_key,
                            global_vars, dag, configured_catalog):
        # Iceberg table creation is handled inside Spark in this environment.
        return []


class DagCommons:

    @staticmethod
    def generate_dag_arguments(airflow_dag_details_from_config, airflow_pipeline_vars):
        dag_arguments = {
            "dag_id": airflow_dag_details_from_config["dag_id"],
            "default_args": {'max_active_runs': 1},
            "description": airflow_dag_details_from_config.get("dag_description", None),
            "schedule": airflow_pipeline_vars.get("schedule", None),
            "start_date": airflow_pipeline_vars["start_date"],
            "catchup": False,
            "tags": airflow_pipeline_vars.get("tags", None)
        }
        return dag_arguments

    @staticmethod
    def read_dag_details(s3_workflows_config_path_prefix):
        airflow_var_globals = ReadAirflowVariables.get_global_vars()

        configs = ReadPipelineConfig.read_config_from_s3(
            s3_workflows_config_path_prefix, airflow_var_globals
        )
        airflow_dag_details_from_config = ReadPipelineConfig.get_airflow_dag_details_from_config(
            configs
        )
        airflow_var_dag_info = ReadAirflowVariables.get_airflow_pipelines_vars(
            airflow_dag_details_from_config
        )

        dag_arguments = DagCommons.generate_dag_arguments(
            airflow_dag_details_from_config, airflow_var_dag_info
        )

        return configs, airflow_var_globals, airflow_var_dag_info, dag_arguments

    @staticmethod
    def build_task_group(dag, configs, global_vars, airflow_pipeline_vars):
        configured_catalog = airflow_pipeline_vars["catalog"]
        airflow_task_details = configs["airflow-task-details"]
        aiflow_task_dependencies_details = (
            configs.get("airflow-task-dependencies-details")
            or configs.get("aiflow-task-dependencies-details")
            or {}
        )
        common_arguments = configs.get("common-arguments", {})

        airflow_tasks_groups_list = []
        for task_group_key in airflow_task_details.keys():

            with TaskGroup(group_id=task_group_key) as tg:

                airflow_tasks_list = []

                for task_details_key in airflow_task_details[task_group_key].keys():

                    if task_details_key == "spark_details":
                        tasks_list = BuildDagTaskGroup.build_spark_details(
                            dag, airflow_task_details, task_group_key,
                            task_details_key, global_vars,
                            airflow_pipeline_vars, common_arguments
                        )
                        airflow_tasks_list.extend(tasks_list)

                    elif task_details_key == "trino_details":
                        tasks_list = BuildDagTaskGroup.build_trino_details(
                            airflow_task_details, task_group_key,
                            task_details_key, global_vars, dag, configured_catalog
                        )
                        airflow_tasks_list.extend(tasks_list)

                for j in range(0, len(airflow_tasks_list)):
                    if j != 0:
                        airflow_tasks_list[j].set_upstream(airflow_tasks_list[j - 1])

            airflow_tasks_groups_list.append((tg, task_group_key))

        DagCommons._build_parent_child_dependencies(
            dag, airflow_tasks_groups_list, aiflow_task_dependencies_details
        )

    @staticmethod
    def _build_parent_child_dependencies(dag, airflow_tasks_groups_list, aiflow_task_dependencies_details):
        all_nodes_list = []
        all_nodes_list.extend(airflow_tasks_groups_list)
        dependency_tuples_list = []

        if "start" not in [n[1] for n in all_nodes_list]:
            all_nodes_list.insert(0, (EmptyOperator(task_id="start", dag=dag), "start"))
        if "end" not in [n[1] for n in all_nodes_list]:
            all_nodes_list.append((EmptyOperator(task_id="end", dag=dag), "end"))

        if aiflow_task_dependencies_details and "parent-child-relation" in aiflow_task_dependencies_details:
            parent_child = aiflow_task_dependencies_details["parent-child-relation"]
            for child_task, parent_tasks in parent_child.items():
                child_node = next(node[0] for node in all_nodes_list if node[1] == child_task)
                for parent_task in parent_tasks:
                    parent_node = next(node[0] for node in all_nodes_list if node[1] == parent_task)
                    dependency_tuples_list.append((parent_node, child_node))
        else:
            if aiflow_task_dependencies_details:
                nodes_names_list = [node_info[1] for node_info in all_nodes_list]
                config_task_names_list = []
                for task_name in aiflow_task_dependencies_details.keys():
                    config_task_names_list.append(task_name)
                    deps = aiflow_task_dependencies_details[task_name]
                    if len(deps) >= 1:
                        config_task_names_list.append(deps[0])
                    if len(deps) >= 2:
                        config_task_names_list.append(deps[1])
                for task_name in set(config_task_names_list):
                    if task_name not in nodes_names_list:
                        all_nodes_list.append((EmptyOperator(task_id=task_name, dag=dag), task_name))
                for config_current_task_name in aiflow_task_dependencies_details.keys():
                    current_node = next(
                        node[0] for node in all_nodes_list if node[1] == config_current_task_name
                    )
                    deps = aiflow_task_dependencies_details[config_current_task_name]
                    predecessor_node = next(
                        node[0] for node in all_nodes_list if node[1] == deps[0]
                    )
                    current_node.set_upstream(predecessor_node)
                    if len(deps) >= 2:
                        successor_node = next(
                            node[0] for node in all_nodes_list if node[1] == deps[1]
                        )
                        current_node.set_downstream(successor_node)
                return

        for predecessor, successor in dependency_tuples_list:
            predecessor >> successor


class EmailAlert:

    @staticmethod
    def on_failure_callback(email_to):
        def send_failure_alert(context):
            task_instance = context['task_instance']
            airflow_url = global_vars["airflow_url"]
            log_url = airflow_url + "/log" + \
                context.get("task_instance").log_url.split("/log")[1]
            title = "PROD : Airflow Alert : {} Dag Failed".format(
                context.get("task_instance").dag_id)
            body = (
                "    Dear Team, <br>    <br>    Failed Task Name: {}. <br>    <br>     "
                "Please check the below mentioned url. <br>    <br>     "
                "Airflow Job Url: <a href={}>{}</a>  "
            ).format(task_instance.task_id, log_url, log_url)
            send_email(email_to, title, body)

        return send_failure_alert


configs, global_vars, airflow_pipeline_vars, dag_arguments = \
    DagCommons.read_dag_details(s3_workflows_config_path_prefix)

with DAG(**dag_arguments) as dag:
    on_failure_callback = EmailAlert.on_failure_callback(dag_default_args.get('email_to'))
    dag_default_args.update({'on_failure_callback': on_failure_callback})
    DagCommons.build_task_group(dag, configs, global_vars, airflow_pipeline_vars)

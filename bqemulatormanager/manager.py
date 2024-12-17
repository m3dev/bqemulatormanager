from typing import Dict, List, Union

import pandas as pd
from google.api_core.client_options import ClientOptions
from google.auth.credentials import AnonymousCredentials
from google.cloud import bigquery

from bqemulatormanager.emulator import Emulator, PortOccupiedError
from bqemulatormanager.schema import SchemaManager


class ManagerError(Exception):
    pass


class Manager:
    def __init__(
        self,
        project: str = "test",
        host: str = "127.0.0.1",
        port: int = 9050,
        grpc_port: int = 9060,
        schema_path: str = "bqem_master_schema.yaml",
        launch_emulator: bool = True,
        debug_mode: bool = False,
        max_pool: int = 20,
    ):
        original_port = port
        grpc_original_port = grpc_port
        for i in range(max_pool):
            port = original_port + i
            grpc_port = grpc_original_port + i
            try:
                self.emulator = Emulator(project, port, grpc_port, launch_emulator=launch_emulator, debug_mode=debug_mode)
            except PortOccupiedError as e:
                print(e)
            else:
                break
        else:
            raise RuntimeError(f"there is no empty port from {original_port} to {port}")

        self.client = self._make_client(project_name=project, host=host, port=port)

        prod_client = bigquery.Client(project, credentials=AnonymousCredentials())

        self.schema_manager = SchemaManager(client=prod_client, schema_file_path=schema_path)
        self.structure: Dict[str, Dict[str, bool]] = {}
        self.project_name = project

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_value, traceback):
        del self.emulator
        del self.schema_manager

    @staticmethod
    def _make_client(project_name: str, host: str, port: int) -> bigquery.Client:
        client_options = ClientOptions(api_endpoint=f"http://{host}:{port}")
        client = bigquery.Client(
            project_name,
            client_options=client_options,
            credentials=AnonymousCredentials(),
        )
        return client

    def load(self, data: pd.DataFrame, path: str):
        dataset, table_id = path.split(".")
        if dataset not in self.structure:
            self.create_dataset(dataset)

        if table_id not in self.structure[dataset]:
            self.create_table(dataset, table_id, [])

        table = self.client.get_table(f"{self.project_name}.{path}")
        self.client.insert_rows_from_dataframe(table, data)

    def create_dataset(self, dataset_name: str, exists_ok=True, timeout: Union[float, None] = None):
        dataset = bigquery.Dataset(f"{self.project_name}.{dataset_name}")
        self.client.create_dataset(dataset, exists_ok=exists_ok, timeout=timeout)
        self.structure[dataset_name] = {}

    def create_table(self, dataset_name: str, table_name: str, schema: List[bigquery.SchemaField], timeout: Union[float, None] = None):
        if schema == []:
            schema = self.schema_manager.get_schema(self.project_name, dataset_name, table_name)
            if schema is None:
                raise ManagerError(f"schema for {dataset_name}.{table_name} is not found in master schema")

        table = bigquery.Table(f"{self.project_name}.{dataset_name}.{table_name}", schema=schema)
        self.client.create_table(table, timeout=timeout)
        self.structure[dataset_name][table_name] = True

    def query(self, sql: str) -> pd.DataFrame:
        return self.client.query(sql).to_dataframe(create_bqstorage_client=False)

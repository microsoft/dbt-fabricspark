from typing import  Dict

from dbt.adapters.base import PythonJobHelper
from dbt.adapters.contracts.connection import Connection
from dbt_common.exceptions import DbtRuntimeError,DbtDatabaseError

from dbt.adapters.fabricspark import SparkCredentials
from dbt.adapters.fabricspark.livysession import LivySessionManager

class BaseFabricSparkHelper(PythonJobHelper):
    """
    Implementation of PythonJobHelper for FabricSpark. 
    """
    def __init__(self, parsed_model: Dict, credentials: SparkCredentials) -> None:
        """
        Initialize Spark Job Submission helper.

        Parameters
        ----------
        parsed_model(Dict)
            A dictionary containing the parsed model information, used to extract various configurations required for job submission.
        credentials(SparkCredentials)
            A SparkCredentials object containing the credentials needed to access the Spark cluster, used to establish the connection.
        """
        self.credentials = credentials
        self.relation_name = parsed_model.get('relation_name')
        self.original_file_path = parsed_model.get('original_file_path')
        self.submission_method = parsed_model.get('config',{}).get('submission_method')
        self.connection = self._get_or_create_connection()
    
    def _get_or_create_connection(self) -> Connection:
        """
        Get the existing Livy connection, or create one using SparkConnectionManager if it does not exist.
        """
        connection = LivySessionManager.connect(self.credentials)
        return connection

    def submit(self, compiled_code: str) -> None:
        """
        Submits compiled code to the database and handles execution results or errors.

        Parameters
        ----------
        compiled_code (str):
            The compiled code string to be executed.

        Raises
        ------
        DbtRuntimeError
        """

        cursor = self.connection.cursor()
        try:
            cursor.execute(compiled_code, 'pyspark')
            for line in cursor.fetchall():
                print(line)
        except DbtDatabaseError as ex:
            raise DbtRuntimeError(f"Unable to create model {self.relation_name}(file: {self.original_file_path}) with a {self.submission_method} type submission. Caused by:\n{ex.msg}")

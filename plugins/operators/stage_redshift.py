from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    """
    Stage to Redshift Operator class.
    Operator which takes the data in an S3 bucket with a certain key and stages
    it to a Redshift table for later processing into the Data Warehouse tables.
    """
    ui_color = '#358140'
    copy_sql = """
    COPY {table}
    FROM '{s3_path}'
    JSON '{jsonpath}'
    ACCESS_KEY_ID '{key}'
    SECRET_ACCESS_KEY '{secret}'
    """

    @apply_defaults
    def __init__(
        self, s3_bucket, s3_key, table,
        aws_credentials_id="aws_credentials", redshift_conn_id="redshift",
        jsonpath=None, *args, **kwargs
    ):
        """
        Stage to Redshift Operator constructor.
        Sets the operator variables depending on arguments passed for correct
        execution of operator.
        """
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.s3_path = f"s3://{s3_bucket}/{s3_key}"
        self.table = table
        self.aws_credentials_id = aws_credentials_id
        self.redshift_conn_id = redshift_conn_id
        self.jsonpath = f"s3://{s3_bucket}/{jsonpath}" \
            if jsonpath is not None else "auto"

    def execute(self, context):
        """
        Execute Operator.
        Execute function for operator, which creates the hooks to AWS and
        Redshift to run the COPY query to load the staging table specified.
        """
        self.log.info("Setting Hooks for AWS and Redshift")
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from destination Redshift table")
        redshift.run(f"DELETE FROM {self.table}")

        self.log.info("Copying data from S3 to Redshift")
        formatted_sql = self.copy_sql.format(
            table=self.table,
            s3_path=self.s3_path,
            jsonpath=self.jsonpath,
            key=credentials.access_key,
            secret=credentials.secret_key
        )
        redshift.run(formatted_sql)

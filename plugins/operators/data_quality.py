from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    """
    Data Quality Operator class.
    Operator which retrieves records from a given query and checks if they
    fulfill a certain condition, to check for data quality.
    """
    ui_color = "#89DA59"

    @apply_defaults
    def __init__(
        self, query, condition, redshift_conn_id="redshift", *args, **kwargs
    ):
        """
        Data Quality Operator constructor.
        Sets the operator variables depending on arguments passed for correct
        execution of operator.
        """
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.query = query
        self.condition = condition
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        """
        Execute Operator.
        Execute function for operator, which creates the hook to Redshift to
        run the specified query, retrieve the results and compare them to the
        given condition.
        """
        self.log.info("Setting Hook for Redshift")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Getting records from query")
        records = redshift.get_records(self.query)

        self.log.info("Checking condition from results")
        if records is None or not eval(self.condition, {"records": records}):
            error = f"Condition {self.condition} not met."
            raise ValueError(error)

        self.log.info(f"Condition {self.condition} passed.")

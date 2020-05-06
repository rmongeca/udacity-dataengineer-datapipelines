from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):
    """
    Load Dimension Operator class.
    Operator which loads the Dimension table specified with the given data,
    provided through a SQL query to the staging tables.
    """
    ui_color = '#80BD9E'
    dimension_insert = """
    INSERT INTO {table}
        ({subquery})
    """

    @apply_defaults
    def __init__(
        self, table, subquery, redshift_conn_id="redshift", *args, **kwargs
    ):
        """
        Load Dimension Operator constructor.
        Sets the operator variables depending on arguments passed for correct
        execution of operator.
        """
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.subquery = subquery
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        """
        Execute Operator.
        Execute function for operator, which creates the hook to Redshift to
        run the INSERT query to load the Dimension table specified.
        """
        self.log.info("Setting Hook for Redshift")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f"Loading Dimension table {self.table}")
        formatted_sql = self.dimension_insert.format(
            table=self.table,
            subquery=self.subquery
        )
        redshift.run(formatted_sql)

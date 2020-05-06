from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):
    """
    Load Fact Operator class.
    Operator which loads the Fact table specified with the given data, provided
    through a SQL query to the staging tables.
    """
    ui_color = '#F98866'
    fact_insert = """
    INSERT INTO {table}
        ({subquery})
    """

    @apply_defaults
    def __init__(
        self, table, subquery, redshift_conn_id="redshift", *args, **kwargs
    ):
        """
        Load Fact Operator constructor.
        Sets the operator variables depending on arguments passed for correct
        execution of operator.
        """
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.subquery = subquery
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        """
        Execute Operator.
        Execute function for operator, which creates the hook to Redshift to
        run the INSERT query to load the Fact table specified.
        """
        self.log.info("Setting Hooks for Redshift")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f"Loading Fact table {self.table}")
        formatted_sql = self.fact_insert.format(
            table=self.table,
            subquery=self.subquery
        )
        redshift.run(formatted_sql)

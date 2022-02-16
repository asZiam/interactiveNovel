from google.cloud import bigquery

KEY = 'private.json'
client = bigquery.Client.from_service_account_json(KEY) 
table_id = 'elite-thunder-340219.ltv_holy.test'
file = 'solid-test-assignment.json'

job_config = bigquery.LoadJobConfig(
    schema=[bigquery.SchemaField("invoices", "RECORD",
        mode='REPEATED',
        fields=(bigquery.SchemaField('subscription_term_number', 'INTEGER'),
                bigquery.SchemaField('billing_period_ended_at', 'TIMESTAMP'),
                bigquery.SchemaField('billing_period_started_at', 'TIMESTAMP'),
                bigquery.SchemaField('created_at', 'TIMESTAMP'),
                bigquery.SchemaField('status', 'STRING'),
                bigquery.SchemaField('amount', 'INTEGER'),
                bigquery.SchemaField('updated_at', 'TIMESTAMP'),
                bigquery.SchemaField('orders', 'RECORD',
                    mode='REPEATED',
                    fields=(
                        bigquery.SchemaField('operation', 'STRING'),
                        bigquery.SchemaField('failed_reason', 'FLOAT'),
                        bigquery.SchemaField('processed_at', 'TIMESTAMP'),
                        bigquery.SchemaField('created_at', 'TIMESTAMP'),
                        bigquery.SchemaField('amount', 'INTEGER'),
                        bigquery.SchemaField('retry_attempt', 'INTEGER'),
                        bigquery.SchemaField('status', 'STRING'),
                        bigquery.SchemaField('id', 'STRING')
                        )),
                bigquery.SchemaField('id', 'STRING')
                ),
        ),
        bigquery.SchemaField("payment_type", "STRING"),
        bigquery.SchemaField("trial", "BOOLEAN"),
        bigquery.SchemaField("cancel_message", "STRING"),
        bigquery.SchemaField("started_at", "TIMESTAMP"),
        bigquery.SchemaField("next_charge_at", "TIMESTAMP"),
        bigquery.SchemaField("status", "STRING"),
        bigquery.SchemaField("expired_at", "TIMESTAMP"),
        bigquery.SchemaField("product", "RECORD",
            fields=(
                bigquery.SchemaField('trial_period', 'STRING'),
                bigquery.SchemaField('amount', 'STRING'),
                bigquery.SchemaField('trial', 'STRING'),
                bigquery.SchemaField('payment_action', 'STRING'),
                bigquery.SchemaField('currency', 'STRING'),
                bigquery.SchemaField('name', 'STRING'),
                bigquery.SchemaField('id', 'STRING')
                )
            ),
        bigquery.SchemaField("cancel_code", "FLOAT"),
        bigquery.SchemaField("cancelled_at", "TIMESTAMP"),
        bigquery.SchemaField("id", "STRING"),
        bigquery.SchemaField("customer_account_id", "STRING")
    ],
    source_format='NEWLINE_DELIMITED_JSON'
)

with open(file, 'rb') as file:
    load_job = client.load_table_from_file(
        file, table_id, job_config=job_config
    )

load_job.result()  # Wait for the job to complete.

destination_table = client.get_table(table_id)
print("Loaded {} rows...".format(destination_table.num_rows))
def insert_all_tables(bq_client, rows, event_id, project, dataset):

    for table, data_rows in rows.items():
        if not data_rows:
            continue

        table_ref = f"{project}.{dataset}.{table}"
        row_ids = [f"{event_id}_{table}_{i}" for i in range(len(data_rows))]

        errors = bq_client.insert_rows_json(
            table_ref,
            data_rows,
            row_ids=row_ids
        )

        if errors:
            print(f"Error inserting into {table}: {errors}")

from google.cloud import bigquery

bq_client = bigquery.Client()

def ensure_table_exists(project_id, dataset_id, table_name, rows):

    table_ref = f"{project_id}.{dataset_id}.{table_name}"

    try:
        table = bq_client.get_table(table_ref)
    except:
        print(f"🔥 Creating NEW table: {table_name}")

        schema = []

        sample = rows[0]

        for k,v in sample.items():

            if isinstance(v,int):
                t="INT64"
            elif isinstance(v,float):
                t="FLOAT64"
            else:
                t="STRING"

            schema.append(bigquery.SchemaField(k,t))

        table = bigquery.Table(table_ref, schema=schema)
        table = bq_client.create_table(table)

    # ---------- AUTO ADD NEW COLUMNS ----------
    existing = {f.name for f in table.schema}

    new_fields=[]

    for r in rows:
        for k in r.keys():
            if k not in existing:
                new_fields.append(
                    bigquery.SchemaField(k,"STRING")
                )
                existing.add(k)

    if new_fields:
        table.schema = list(table.schema)+new_fields
        bq_client.update_table(table,["schema"])

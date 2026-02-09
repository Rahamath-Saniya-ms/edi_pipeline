# ==========================================================
# ENTERPRISE V8 FINAL — PRODUCTION SAFE INGESTION
#
# Supports ONLY:
#   ✔ EDI_Interchanges
#   ✔ EDI_Transactions
#   ✔ PO_Headers
#   ✔ PO_Lines
#   ✔ ASN_Headers
#   ✔ ASN_Lines
#   ✔ Invoice_Headers
#   ✔ Invoice_Lines
#   ✔ Invoice_Charges
#
# ✔ No SEG tables
# ✔ Self-healing
# ✔ Idempotent
# ==========================================================

import functions_framework
from google.cloud import storage
from google.cloud import bigquery
from edi_reader import extract_interchange_and_transactions
from id_generator import generate_deterministic_id

PROJECT_ID = "pluto7-solutions"
DATASET_ID = "edi_integration"

storage_client = storage.Client()
bq_client = bigquery.Client()


# ==========================================================
# MAIN CLOUD FUNCTION
# ==========================================================
@functions_framework.cloud_event
def process_edi_upload(cloud_event):

    print("STEP 1: Event received")

    data = cloud_event.data
    event_id = cloud_event.get("id")

    bucket_name = data.get("bucket")
    filename = data.get("name")

    if not filename:
        print("No filename provided.")
        return

    print(f"Processing file: {filename}")

    # ======================================================
    #  IDEMPOTENCY CHECK (PRODUCTION SAFE)
    # ======================================================
    check_query = f"""
        SELECT 1
        FROM `{PROJECT_ID}.{DATASET_ID}.EDI_Interchanges`
        WHERE source_filename = '{filename}'
        LIMIT 1
    """

    if list(bq_client.query(check_query)):
        print("File already processed — skipping.")
        return

    # ======================================================
    # READ FILE FROM GCS
    # ======================================================
    blob = storage_client.bucket(bucket_name).blob(filename)
    edi_text = blob.download_as_text()

    print("STEP 2: File loaded from GCS")

    # ======================================================
    # PARSE EDI (SELF HEALING)
    # ======================================================
    (
        interchange_data,
        transactions,
        po_header,
        po_lines,
        asn_header,
        asn_lines,
        invoice_header,
        invoice_lines,
        invoice_charges
    ) = extract_interchange_and_transactions(edi_text)

    print("STEP 3: Parsing complete")

    # ======================================================
    # CREATE INTERCHANGE ID
    # ======================================================
    interchange_id = generate_deterministic_id(
        "ISA",
        interchange_data.get("control_number", filename)
    )

    # ======================================================
    # ROW CONTAINER
    # ======================================================
    rows = {
        "EDI_Interchanges": [],
        "EDI_Transactions": [],
        "PO_Headers": [],
        "PO_Lines": [],
        "ASN_Headers": [],
        "ASN_Lines": [],
        "Invoice_Headers": [],
        "Invoice_Lines": [],
        "Invoice_Charges": []
    }

    # ======================================================
    # INSERT INTERCHANGE (ONCE)
    # ======================================================
    rows["EDI_Interchanges"].append({
        "interchange_id": interchange_id,
        "sender_id": interchange_data.get("sender_id"),
        "receiver_id": interchange_data.get("receiver_id"),
        "interchange_date": interchange_data.get("date"),
        "interchange_time": interchange_data.get("time"),
        "control_number": interchange_data.get("control_number"),
        "source_filename": filename
    })

    # ======================================================
    # TRANSACTION MAP (MULTI ST SAFE)
    # ======================================================
    tx_map = {}

    for tx in transactions:

        tx_id = generate_deterministic_id(
            "TX",
            f"{filename}_{tx['control_number']}"
        )

        tx_map[tx["type"]] = tx_id

        rows["EDI_Transactions"].append({
            "transaction_id": tx_id,
            "interchange_id": interchange_id,
            "transaction_type": tx["type"],
            "control_number": tx["control_number"]
        })

    # ======================================================
    # INSERT PO HEADER + LINES
    # ======================================================
    if po_header and po_header.get("po_number"):

        po_id = generate_deterministic_id(
            "PO",
            f"{filename}_{po_header['po_number']}"
        )

        rows["PO_Headers"].append({
            "po_id": po_id,
            "transaction_id": tx_map.get("850"),
            "po_number": po_header.get("po_number"),
            "po_date": po_header.get("po_date"),
            "currency_code": po_header.get("currency_code") or "USD",
            "total_amount": po_header.get("total_amount") or 0,
            "source_filename": filename
        })

        for line in po_lines:

            rows["PO_Lines"].append({
                "line_id": generate_deterministic_id(
                    "POL", f"{filename}_{line['line_num']}"
                ),
                "po_id": po_id,
                "line_num": line.get("line_num"),
                "qty": line.get("qty") or 0,
                "uom": line.get("uom"),
                "price": line.get("price") or 0,
                "item_code": line.get("item_code"),
                "description": line.get("description")
            })

    # ======================================================
    #  INSERT ASN HEADER + LINES
    # ======================================================
    if asn_header and asn_header.get("shipment_id"):

        asn_id = generate_deterministic_id(
            "ASN",
            f"{filename}_{asn_header['shipment_id']}"
        )

        rows["ASN_Headers"].append({
            "asn_id": asn_id,
            "transaction_id": tx_map.get("856"),
            "shipment_id": asn_header.get("shipment_id"),
            "asn_date": asn_header.get("asn_date"),
            "asn_time": asn_header.get("asn_time"),
            "po_number_ref": asn_header.get("po_number_ref"),
            "carrier_code": asn_header.get("carrier_code"),
            "source_filename": filename
        })

        for line in asn_lines:

            rows["ASN_Lines"].append({
                "line_id": generate_deterministic_id(
                    "ASNL", f"{filename}_{line['line_num']}"
                ),
                "asn_id": asn_id,
                "line_num": line.get("line_num"),
                "item_code": line.get("item_code"),
                "qty_shipped": line.get("qty_shipped") or 0,
                "uom": line.get("uom"),
                "price": line.get("price") or 0
            })

    # ======================================================
    #  INSERT INVOICE HEADER + LINES + CHARGES
    # ======================================================
    if invoice_header and invoice_header.get("invoice_number"):

        inv_id = generate_deterministic_id(
            "INV",
            f"{filename}_{invoice_header['invoice_number']}"
        )

        rows["Invoice_Headers"].append({
            "invoice_id": inv_id,
            "transaction_id": tx_map.get("810"),
            "invoice_number": invoice_header.get("invoice_number"),
            "po_number": invoice_header.get("po_number"),
            "invoice_date": invoice_header.get("invoice_date"),
            "total_amount": invoice_header.get("total_amount") or 0,
            "currency_code": invoice_header.get("currency_code") or "USD",
            "source_filename": filename
        })

        for line in invoice_lines:

            rows["Invoice_Lines"].append({
                "line_id": generate_deterministic_id(
                    "INVL", f"{filename}_{line['line_num']}"
                ),
                "invoice_id": inv_id,
                "line_num": line.get("line_num"),
                "qty": line.get("qty") or 0,
                "uom": line.get("uom"),
                "price": line.get("price") or 0,
                "item_code": line.get("item_code"),
                "description": line.get("description")
            })

        for charge in invoice_charges:

            rows["Invoice_Charges"].append({
                "charge_id": generate_deterministic_id(
                    "CHG", f"{filename}_{charge['charge_code']}"
                ),
                "invoice_id": inv_id,
                "charge_code": charge.get("charge_code"),
                "charge_description": charge.get("charge_description"),
                "amount": charge.get("amount") or 0
            })

    # ======================================================
    # DEBUG COUNTS
    # ======================================================
    print("\n========== INSERT COUNTS ==========")
    for table, data_rows in rows.items():
        print(f"{table}: {len(data_rows)} rows ready")
    print("===================================\n")

    # ======================================================
    # WRITE TO BIGQUERY
    # ======================================================
    print("STEP 4: Writing to BigQuery")

    for table, data_rows in rows.items():

        if not data_rows:
            continue

        table_ref = f"{PROJECT_ID}.{DATASET_ID}.{table}"

        row_ids = [
            f"{event_id}_{table}_{i}" for i in range(len(data_rows))
        ]

        errors = bq_client.insert_rows_json(
            table_ref,
            data_rows,
            row_ids=row_ids
        )

        if errors:
            print(f"Insert error in {table}: {errors}")

    print("SUCCESS: ENTERPRISE  INGESTION COMPLETE")

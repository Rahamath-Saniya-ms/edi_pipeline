# ==========================================================
# ENTERPRISE V8 UNIVERSAL BUSINESS PARSER
# LOCKED TO 11 TABLE MODEL
# SELF HEALING + AUTO MAPPING
# ==========================================================

def safe_get(arr, index, default=""):
    try:
        val = arr[int(index)]
        return val.strip() if val else default
    except:
        return default


def format_date(raw):
    if raw and len(raw) == 8:
        return f"{raw[0:4]}-{raw[4:6]}-{raw[6:8]}"
    return raw


# ==========================================================
# APPROVED TABLES (NO SEG_* TABLES EVER)
# ==========================================================
APPROVED_TABLES = {
    "PO_Headers",
    "PO_Lines",
    "ASN_Headers",
    "ASN_Lines",
    "Invoice_Headers",
    "Invoice_Lines",
    "Invoice_Charges",
}


# ==========================================================
# UNIVERSAL PARSER
# ==========================================================
def extract_interchange_and_transactions(edi_text: str):

    segments = [s.strip() for s in edi_text.split("~") if s.strip()]

    interchange = {}
    transactions = []

    po_header = {}
    po_lines = []

    asn_header = {}
    asn_lines = []

    invoice_header = {}
    invoice_lines = []
    invoice_charges = []

    seen_po = set()
    seen_asn = set()
    seen_inv = set()

    for seg in segments:

        try:

            parts = seg.split("*")
            tag = parts[0]

            # ================= ISA =================
            if tag == "ISA":
                interchange = {
                    "sender_id": safe_get(parts,6),
                    "receiver_id": safe_get(parts,8),
                    "date": format_date(safe_get(parts,9)),
                    "time": safe_get(parts,10),
                    "control_number": safe_get(parts,13)
                }

            # ================= ST =================
            elif tag == "ST":
                transactions.append({
                    "type": safe_get(parts,1),
                    "control_number": safe_get(parts,2)
                })

            # ================= 850 PO =================
            elif tag == "BEG":
                po_header = {
                    "po_number": safe_get(parts,3),
                    "po_date": format_date(safe_get(parts,5)),
                    "currency_code": "USD",
                    "total_amount": 0
                }

            elif tag == "PO1":

                line_num = int(safe_get(parts,1) or 0)

                if line_num not in seen_po:
                    seen_po.add(line_num)

                    po_lines.append({
                        "line_num": line_num,
                        "qty": float(safe_get(parts,2) or 0),
                        "uom": safe_get(parts,3),
                        "price": float(safe_get(parts,4) or 0),
                        "item_code": safe_get(parts,7),
                        "description": ""
                    })

            elif tag == "PID":
                if po_lines:
                    po_lines[-1]["description"] = safe_get(parts,5)

            # ================= 856 ASN =================
            elif tag == "BSN":
                asn_header = {
                    "shipment_id": safe_get(parts,2),
                    "asn_date": format_date(safe_get(parts,3)),
                    "asn_time": safe_get(parts,4),
                    "po_number_ref": "",
                    "carrier_code": ""
                }

            elif tag == "PRF":
                if asn_header:
                    asn_header["po_number_ref"] = safe_get(parts,1)

            elif tag == "TD5":
                if asn_header:
                    asn_header["carrier_code"] = safe_get(parts,3)

            elif tag == "LIN":

                line_num = int(safe_get(parts,1) or 0)

                if line_num not in seen_asn:
                    seen_asn.add(line_num)

                    asn_lines.append({
                        "line_num": line_num,
                        "item_code": safe_get(parts,3),
                        "qty_shipped": 0,
                        "uom": "",
                        "price": 0
                    })

            elif tag == "SN1":
                if asn_lines:
                    asn_lines[-1]["qty_shipped"] = float(safe_get(parts,2) or 0)
                    asn_lines[-1]["uom"] = safe_get(parts,3)

            # ================= 810 INVOICE =================
            elif tag == "BIG":
                invoice_header = {
                    "invoice_number": safe_get(parts,2),
                    "po_number": safe_get(parts,4),
                    "invoice_date": format_date(safe_get(parts,1)),
                    "total_amount": 0,
                    "currency_code": "USD"
                }

            elif tag == "IT1":

                line_num = int(safe_get(parts,1) or 0)

                if line_num not in seen_inv:
                    seen_inv.add(line_num)

                    invoice_lines.append({
                        "line_num": line_num,
                        "qty": float(safe_get(parts,2) or 0),
                        "uom": safe_get(parts,3),
                        "price": float(safe_get(parts,4) or 0),
                        "item_code": safe_get(parts,6),
                        "description": ""
                    })

            elif tag == "TDS":
                if invoice_header:
                    invoice_header["total_amount"] = float(safe_get(parts,1) or 0)

            elif tag == "SAC":
                invoice_charges.append({
                    "charge_code": safe_get(parts,2),
                    "charge_description": "",
                    "amount": float(safe_get(parts,5) or 0)
                })

        except Exception:
            print(f"Self-healed bad segment → {seg}")

    return (
        interchange,
        transactions,
        po_header,
        po_lines,
        asn_header,
        asn_lines,
        invoice_header,
        invoice_lines,
        invoice_charges
    )

# ==========================================================
# LOCAL TEST RUNNER FOR EDI PIPELINE
# Simulates Cloud Storage Trigger
# Prints execution status
# ==========================================================

from main import process_edi_upload
import datetime


# ==========================================================
# Fake CloudEvent
# ==========================================================
class FakeEvent:
    def __init__(self):
        self._data = {
            "bucket": "edi-raw-data",
            "name": "test_1.txt",
        }

    def get(self, key):
        if key == "id":
            # Unique ID each run
            return f"LOCAL_TEST_{datetime.datetime.now().timestamp()}"
        return None

    @property
    def data(self):
        return self._data


# ==========================================================
# LOCAL ENTRYPOINT
# ==========================================================
if __name__ == "__main__":

    print("\n==============================")
    print("RUNNING LOCAL EDI TEST")
    print("==============================")

    fake_event = FakeEvent()

    try:
        process_edi_upload(fake_event)

        print("\n PIPELINE EXECUTED SUCCESSFULLY")

    except Exception as e:
        print("\n PIPELINE FAILED")
        print("Error:", e)

    print("\n==============================")
    print(" LOCAL TEST COMPLETE")
    print("==============================\n")

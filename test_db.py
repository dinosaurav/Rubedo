from batchbrain.db import get_session, init_db
from batchbrain.models import RunCoordinateStatus
import os
os.environ["BATCHBRAIN_DB_PATH"] = f"sqlite:///.test_concurrency_env/batchbrain.sqlite"
init_db()
for s in get_session().query(RunCoordinateStatus).filter_by(status='failed').all():
    print(s.error_message)

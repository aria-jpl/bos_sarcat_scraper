# from hysds.dataset_ingest import ingest
# from hysds.celery import app
import os
import shutil

def ingest_acq_dataset(ds_cfg = "/home/ops/verdi/etc/datasets.json"):
    """Ingest acquisition dataset."""
    for dir in os.listdir('.'):
        if os.path.isdir(dir):
            id = dir
            try:
                if id.startswith("acquisition-"):
                    # ingest(id, ds_cfg, app.conf.GRQ_UPDATE_URL, app.conf.DATASET_PROCESSED_QUEUE, dir, None)
                    print("Ingesting {}".format(id))
                    shutil.rmtree(id)
            except Exception as e:
                print ("Failed to ingest dataset {}".format(id))
    return
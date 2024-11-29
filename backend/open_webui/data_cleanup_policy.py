from contextlib import contextmanager
from datetime import datetime, timedelta
import logging
import os
from pathlib import Path
import time

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.triggers.cron import CronTrigger
from sqlalchemy import JSON, Column, String
from sqlalchemy.exc import IntegrityError

from open_webui.env import DATABASE_URL, SRC_LOG_LEVELS
from open_webui.config import CACHE_DIR, UPLOAD_DIR
from open_webui.apps.retrieval.vector.connector import VECTOR_DB_CLIENT
from open_webui.apps.webui.internal.db import Base
from open_webui.apps.webui.models.chats import Chat
from open_webui.apps.webui.models.files import File
from open_webui.apps.webui.internal.db import get_db
from open_webui.storage.provider import Storage

log = logging.getLogger(__name__)
log.setLevel(SRC_LOG_LEVELS["DATA_CLEANUP"])

DATA_CLEANUP_ENABLED = os.getenv("DATA_CLEANUP_ENABLED", "false").lower() == "true"
DATA_CLEANUP_CRON_SCHEDULE = os.environ.get(
    "DATA_CLEANUP_CRON_SCHEDULE", "0 * * * *"  # defaults to once per hour
)
if DATA_CLEANUP_ENABLED:
    assert (
        "DATA_CLEANUP_MAX_CHAT_AGE_DAYS" in os.environ
    ), "If DATA_CLEANUP_ENABLED is set, DATA_CLEANUP_MAX_CHAT_AGE_DAYS must be set"
    assert (
        "DATA_CLEANUP_MAX_CACHE_AGE_DAYS" in os.environ
    ), "If DATA_CLEANUP_ENABLED is set, DATA_CLEANUP_MAX_CACHE_AGE_DAYS must be set"
    DATA_CLEANUP_MAX_CHAT_AGE_DAYS = float(os.environ["DATA_CLEANUP_MAX_CHAT_AGE_DAYS"])
    DATA_CLEANUP_MAX_CACHE_AGE_DAYS = float(
        os.environ["DATA_CLEANUP_MAX_CACHE_AGE_DAYS"]
    )


async def setup_data_cleanup_schedule():
    assert DATA_CLEANUP_ENABLED
    log.info(
        f"Data cleanup policy enabled. "
        f"Chats older than {DATA_CLEANUP_MAX_CHAT_AGE_DAYS} days will be deleted. "
        f"Cache files older than {DATA_CLEANUP_MAX_CACHE_AGE_DAYS} days will be deleted. "
        f"Running on cron schedule '{DATA_CLEANUP_CRON_SCHEDULE}'"
    )
    scheduler = AsyncIOScheduler()
    # use sqlalchemy job store to sync across multiple instances
    scheduler.add_jobstore(
        SQLAlchemyJobStore(DATABASE_URL, tablename="data_cleanup_jobs")
    )
    # keep only the jobs defined in this function
    scheduler.start()
    scheduler.remove_all_jobs()
    scheduler.add_job(
        apply_retention_policy,
        CronTrigger.from_crontab(DATA_CLEANUP_CRON_SCHEDULE),
        id="data_cleanup_job",
        max_instances=1,
        coalesce=True,
        replace_existing=True,
    )


def apply_retention_policy():
    with SQLAlchemyMutex.try_acquire_exclusive_lock("data_cleanup_job") as acquired:
        if acquired:
            log.info("Applying data cleanup policy")
            delete_chats_and_uploads()
            delete_cache_files()


class SQLAlchemyMutex(Base):
    __tablename__ = "sqlalchemy_mutex"
    id = Column(String, primary_key=True)

    @classmethod
    @contextmanager
    def try_acquire_exclusive_lock(cls, id: str):
        """
        Acquire an exclusive lock for the given id.
        If the lock is already acquired, the context manager will return immediately without blocking.
        """
        # add some jitter to reduce likelihood of race conditions
        lock = cls(id=id)
        try:
            with get_db() as db:
                log.debug(f"Acquiring lock for id: {id}")
                db.add(lock)
                db.commit()
                acquired = True
                log.debug(f"Acquired lock for id: {id}")
        except IntegrityError:
            acquired = False
            log.debug(f"Lock for id {id} already acquired")

        time.sleep(1)  # allow time for other instances to get IntegrityError
        yield acquired

        if acquired:
            with get_db() as db:
                db.delete(lock)
                db.commit()
                log.debug(f"Released lock for id: {id}")

with get_db() as db:
    SQLAlchemyMutex.__table__.create(db.connection(), checkfirst=True)


def delete_chats_and_uploads():
    """
    Deletes chats older than DATA_CLEANUP_MAX_CHAT_AGE_DAYS,
        their uploaded files from the database & storage,
        and the associated collections from the vector db.
    """
    min_chat_created_at = datetime.now() - timedelta(
        days=DATA_CLEANUP_MAX_CHAT_AGE_DAYS
    )
    log.debug(f"Retiring chats older than {min_chat_created_at}")

    with get_db() as db:
        # get the file ids uploaded to the chats being deleted
        deleting_file_ids: list[str] = [
            file["id"]
            for row in (
                db.query(Chat.chat["files"]).where(
                    Chat.created_at < min_chat_created_at,
                    Chat.chat["files"] != JSON.NULL,
                )
            )
            for file in row._t[0]
        ]

        log.info(
            f"Deleting {len(deleting_file_ids)} file rows and collections from vector db"
        )
        for file_id in deleting_file_ids:
            [(filepath, collection_name)] = db.query(
                File.path, File.meta["collection_name"]
            ).filter(File.id == file_id)

            Storage.delete_file(filepath.replace(UPLOAD_DIR, ""))
            VECTOR_DB_CLIENT.delete_collection(collection_name)

            # delete the row from file table
            n_file_rows_deleted = db.query(File).filter(File.id == file_id).delete()
            assert n_file_rows_deleted == 1

        # delete the rows from chat table
        n_chats_deleted = (
            db.query(Chat).where(Chat.created_at < min_chat_created_at).delete()
        )
        log.info(f"Deleted {n_chats_deleted} chat rows from the db")

        db.commit()


def delete_cache_files():
    "Deletes audio and image files in the /app/data/cache/** folder that are older than DATA_CLEANUP_MAX_CACHE_AGE_DAYS"
    FOLDERS_TO_CLEAN = ["audio", "image"]

    cache_paths = [
        fp
        for folder in FOLDERS_TO_CLEAN
        for fp in Path(CACHE_DIR).glob(f"{folder}/**/*")
        if fp.is_file()
    ]
    min_ctime = (
        datetime.now() - timedelta(days=DATA_CLEANUP_MAX_CACHE_AGE_DAYS)
    ).timestamp()
    log.info(f"Found {len(cache_paths)} total cache files in {CACHE_DIR}")
    log.debug(f"Cache files created before {min_ctime} will be deleted")

    c = 0
    for fp in cache_paths:
        ctime = fp.stat().st_ctime
        log.debug(f"- {fp}: {ctime}")
        if ctime < min_ctime:
            fp.unlink()
            c += 1

    log.info(f"Deleted {c} cache files")

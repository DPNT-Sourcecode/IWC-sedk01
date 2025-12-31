from dataclasses import dataclass
from datetime import datetime
from enum import IntEnum

# LEGACY CODE ASSET
# RESOLVED on deploy
from solutions.IWC.task_types import TaskSubmission, TaskDispatch

class Priority(IntEnum):
    """Represents the queue ordering tiers observed in the legacy system."""
    HIGH = 1
    NORMAL = 2

@dataclass
class Provider:
    name: str
    base_url: str
    depends_on: list[str]

MAX_TIMESTAMP = datetime.max.replace(tzinfo=None)

COMPANIES_HOUSE_PROVIDER = Provider(
    name="companies_house", base_url="https://fake.companieshouse.co.uk", depends_on=[]
)


CREDIT_CHECK_PROVIDER = Provider(
    name="credit_check",
    base_url="https://fake.creditcheck.co.uk",
    depends_on=["companies_house"],
)


BANK_STATEMENTS_PROVIDER = Provider(
    name="bank_statements", base_url="https://fake.bankstatements.co.uk", depends_on=[]
)

ID_VERIFICATION_PROVIDER = Provider(
    name="id_verification", base_url="https://fake.idv.co.uk", depends_on=[]
)


REGISTERED_PROVIDERS: list[Provider] = [
    BANK_STATEMENTS_PROVIDER,
    COMPANIES_HOUSE_PROVIDER,
    CREDIT_CHECK_PROVIDER,
    ID_VERIFICATION_PROVIDER,
]

class Queue:
    def __init__(self):
        self._queue = []

    def _collect_dependencies(self, task: TaskSubmission) -> list[TaskSubmission]:
        provider = next((p for p in REGISTERED_PROVIDERS if p.name == task.provider), None)
        if provider is None:
            return []

        tasks: list[TaskSubmission] = []
        for dependency in provider.depends_on:
            dependency_task = TaskSubmission(
                provider=dependency,
                user_id=task.user_id,
                timestamp=task.timestamp,
            )
            tasks.extend(self._collect_dependencies(dependency_task))
            tasks.append(dependency_task)
        return tasks

    @staticmethod
    def _priority_for_task(task):
        metadata = task.metadata
        raw_priority = metadata.get("priority", Priority.NORMAL)
        try:
            return Priority(raw_priority)
        except (TypeError, ValueError):
            return Priority.NORMAL

    @staticmethod
    def _earliest_group_timestamp_for_task(task):
        metadata = task.metadata
        return metadata.get("group_earliest_timestamp", MAX_TIMESTAMP)

    @staticmethod
    def _timestamp_for_task(task):
        timestamp = task.timestamp
        if isinstance(timestamp, datetime):
            return timestamp.replace(tzinfo=None)
        if isinstance(timestamp, str):
            return datetime.fromisoformat(timestamp).replace(tzinfo=None)
        return timestamp
    
    def _find_existing_index(self, user_id: int, provider: str):
        for idx, t in enumerate(self._queue):
            if t.user_id == user_id and t.provider == provider:
                return idx
        return None 

    def enqueue(self, item: TaskSubmission) -> int:
        tasks = [*self._collect_dependencies(item), item]

        # track seen tasks
        seen_in_batch: set[tuple[int, str]] = set()

        for task in tasks:
            metadata = task.metadata
            metadata.setdefault("priority", Priority.NORMAL)
            metadata.setdefault("group_earliest_timestamp", MAX_TIMESTAMP)

            key = (task.user_id, task.provider)
            if key in seen_in_batch:
                # duplicate within same enqueue call -> resolve by timestamp ordering
                # keep the earliest timestamp, skip is this one is later or replace if earlier
                existing_idx = self._find_existing_index(task.user_id, task.provider)
                if existing_idx is not None:
                    existing_ts = self._timestamp_for_task(self._queue[existing_idx])
                    new_ts = self._timestamp_for_task(task)
                    if new_ts < existing_ts:
                        # replace the queue with earlier task
                        self._queue[existing_idx] = task
                continue

            seen_in_batch.add(key)

            existing_idx = self._find_existing_index(task.user_id, task.provider)
            if existing_idx is None:
                # no duplicate in queue, append
                self._queue.append(task)
            else:
                # duplicate exists in queue, resolve by timestamp ordering and keep earliest timestamp
                existing_task = self._queue[existing_idx]
                existing_ts = self._timestamp_for_task(existing_task)
                new_ts = self._timestamp_for_task(task)
                if new_ts < existing_ts:
                    # replace the queue with earlier task
                    self._queue[existing_idx] = task

        return self.size

    def dequeue(self):
        if self.size == 0:
            return None

        user_ids = {task.user_id for task in self._queue}
        task_count = {}
        priority_timestamps = {}
        for user_id in user_ids:
            user_tasks = [t for t in self._queue if t.user_id == user_id]
            # user normalised timestamp for accurate ordering
            earliest_timestamp = min(self._timestamp_for_task(t) for t in user_tasks)
            priority_timestamps[user_id] = earliest_timestamp
            task_count[user_id] = len(user_tasks)

        for task in self._queue:
            metadata = task.metadata
            current_earliest = metadata.get("group_earliest_timestamp", MAX_TIMESTAMP)
            raw_priority = metadata.get("priority")
            try:
                priority_level = Priority(raw_priority)
            except (TypeError, ValueError):
                priority_level = None

            if priority_level is None or priority_level == Priority.NORMAL:
                metadata["group_earliest_timestamp"] = MAX_TIMESTAMP
                if task_count[task.user_id] >= 3:
                    metadata["group_earliest_timestamp"] = priority_timestamps[task.user_id]
                    metadata["priority"] = Priority.HIGH
                else:
                    metadata["priority"] = Priority.NORMAL
            else:
                metadata["group_earliest_timestamp"] = current_earliest
                metadata["priority"] = priority_level
        
        # determine queue internal age once
        queue_internal_age = self.age


        # Enforce bank statements deprioritisation
        # - if user has < 3 tasks, bank_statements tasks go to end of global queue
        # - if user has >= 3 tasks, bank_statements tasks get scheduled after user other tasks
        def sort_key(i):
            priority = self._priority_for_task(i)
            group_ts = self._earliest_group_timestamp_for_task(i)
            is_bank = (i.provider == "bank_statements")
            user_tasks = task_count.get(i.user_id, 0)

            global_bank_penalty = 0 if (is_bank and user_tasks < 3) else 1
            per_user_bank_penalty = 1 if (is_bank and user_tasks >= 3) else 0

            # If queue internal age is >= 5 minutes, bank_statements become time-sensitive
            # allow them to move earlier by removing penalties, but stil won't skip
            # tasks thar have an older timestam because timestamp remains a later sort key.

            if is_bank and queue_internal_age >= 300:
                global_bank_penalty = 0
                per_user_bank_penalty = 0

            ts = self._timestamp_for_task(i)
            return (priority, group_ts, global_bank_penalty, per_user_bank_penalty, ts)


        self._queue.sort(key=sort_key)

        task = self._queue.pop(0)

        return TaskDispatch(
            provider=task.provider,
            user_id=task.user_id,
        )

    @property
    def size(self):
        return len(self._queue)

    @property
    def age(self):
        """
        Returns the age in seconds of the oldest task in the queue.
        
        Age is the gap between the newest and oldest task timestamps.
        If the queue is empty, returns 0.
        """
        if not self._queue:
            return 0

        timestamps = [self._timestamp_for_task(t) for t in self._queue]
        oldest = min(timestamps)
        newest = max(timestamps)
        delta_seconds = (newest - oldest).total_seconds()
        return int(delta_seconds)

    def purge(self):
        self._queue.clear()
        return True

"""
===================================================================================================

The following code is only to visualise the final usecase.
No changes are needed past this point.

To test the correct behaviour of the queue system, import the `Queue` class directly in your tests.

===================================================================================================

```python
import asyncio
import logging
from fastapi import FastAPI
from contextlib import asynccontextmanager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(queue_worker())
    yield
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        logger.info("Queue worker cancelled on shutdown.")


app = FastAPI(lifespan=lifespan)
queue = Queue()


@app.get("/")
def read_root():
    return {
        "registered_providers": [
            {"name": p.name, "base_url": p.base_url} for p in registered_providers
        ]
    }


class DataRequest(BaseModel):
    user_id: int
    providers: list[str]


@app.post("/fetch_customer_data")
def fetch_customer_data(data: DataRequest):
    provider_names = [p.name for p in registered_providers]

    for provider in data.providers:
        if provider not in provider_names:
            logger.warning(f"Provider {provider} doesn't exists. Skipping")
            continue

        queue.enqueue(
            TaskSubmission(
                provider=provider,
                user_id=data.user_id,
                timestamp=datetime.now(),
            )
        )

    return {"status": f"{len(data.providers)} Task(s) added to queue"}


async def queue_worker():
    while True:
        if queue.size == 0:
            await asyncio.sleep(1)
            continue

        task = queue.dequeue()
        if not task:
            continue

        logger.info(f"Processing task: {task}")
        await asyncio.sleep(2)
        logger.info(f"Finished task: {task}")
```
"""




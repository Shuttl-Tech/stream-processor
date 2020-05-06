# shuttl-workflows
This Python module provides an easy syntax for manipulating, querying, scheduling, batch processing and handling tasks.

## Batching Tasks
Batch processing is a technique of processing data that occur in one large group instead of individually. Batch processing is usually done to help conserve system resources and allow for any modifications before being processed.

```python
from shuttl_workflows import Source, SchedulerFactory, Task

thread_pool_scheduler = SchedulerFactory("ThreadPoolScheduler", max_workers=3)

def hello_world(x):
    return f"{x} Hello World"


def hello_success(response):
    print(f"Successfully executed with response {response}")


def hello_error(exception):
    raise exception


data = [1, 2, 3, 4, 5, 6]
response = (
    Source(data)
    .batch(3)
    .map(
        Task(hello_world, on_success=hello_success, on_error=hello_error),
        scheduler=thread_pool_scheduler,
    )
)
print(list(response))
# => ["(1, 2, 3) Hello World", "(4, 5, 6) Hello World"]
# Successfully executed with response (1, 2, 3) Hello World"
# Successfully executed with response (4, 5, 6) Hello World"
```

## Schedule tasks
Here we can choose between the different type of execution like ThreadPool, ProcessPool and AsyncIO.
Soon we will add support for tasks like gevent, eventlet and greenlet
```python
import asyncio
from shuttl_workflows import Source, SchedulerFactory, Task

task = Task(
    lambda x: x * 2,
    on_success=lambda x: print(f"Executed with result {x}"),
    on_error=lambda err: print(err),
)

thread_pool_scheduler = SchedulerFactory("ThreadPoolScheduler", max_workers=20)
thread_pool_scheduler.add_task(task, 10)
thread_pool_scheduler.add_task(task, "A")
thread_pool_scheduler.results()
# => (20, "AA")
# Executed with result 20
# Executed with result "AA"

process_pool_scheduler = SchedulerFactory("ProcessPoolScheduler", max_workers=20)
process_pool_scheduler.add_task(task, 20)
thread_pool_scheduler.add_task(task, "B")
process_pool_scheduler.results()
# => (40, "BB")
# Executed with result 40
# Executed with result "BB"

event_loop_scheduler = SchedulerFactory(
    "AsyncIOScheduler", loop=asyncio.get_event_loop(), type="uvloop"
)
event_loop_scheduler.add_task(task, 30)
thread_pool_scheduler.add_task(task, "C")
event_loop_scheduler.results()
# => (60, "CC")
# Executed with result 60
# Executed with result CC
```

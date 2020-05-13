# shuttl-workflows
This Python module provides an easy syntax for manipulating, querying, scheduling, batch processing and handling tasks.

## Batching Tasks
Batch processing is a technique of processing data that occur in one large group instead of individually. Batch processing is usually done to help conserve system resources and allow for any modifications before being processed.

```python
from stream_processor.stream import Stream
from stream_processor.schedulers import ThreadPoolScheduler 
from stream_processor.tasks import Task

thread_pool_scheduler = ThreadPoolScheduler(max_workers=3)

def hello_world(x, context=None):
    context["message"] = "Hello World"
    return f"{x} Hello World"


def hello_success(response, context=None):
    print(f"Message from context {context['message']}")
    print(f"Successfully executed with response {response}")


def hello_error(exception, context=None):
    raise exception


data = [1, 2, 3, 4, 5, 6]
response = (
    Stream(data)
    .batch(3)
    .map(
        Task(
            hello_world, 
            on_completion_success_handlers=[hello_success], 
            on_failure_handlers=[hello_error]
        ),
        scheduler=thread_pool_scheduler,
    )
)
list(response)

# Message from context Hello World
#
# Successfully executed with response [1, 2, 3] Hello World is the result
#
# Message from context Hello World
#
# Successfully executed with response [4, 5, 6] Hello World is the result
# => ['[1, 2, 3] Hello World is the result', '[4, 5, 6] Hello World is the result']
```

## Schedule tasks
Here we can choose between the different type of execution like ThreadPool, ProcessPool and AsyncIO.
Currently we only support ThreadPool and Serial but soon other options will be added.
```python
from stream_processor.schedulers import ThreadPoolScheduler
from stream_processor.tasks import Task


task_1 = Task(
    lambda x: x * 2,
    on_completion_success_handlers=[lambda x: print(f"Executed with result {x}")],
    on_failure_handlers=[lambda err: print(err)],
)

task_2 = Task(
    lambda x: x * 3,
    on_completion_success_handlers=[lambda x: print(f"Executed with result {x}")],
    on_failure_handlers=[lambda err: print(err)],
)

thread_pool_scheduler = ThreadPoolScheduler(max_workers=20)
thread_pool_scheduler.add_task(task_1, 10)
thread_pool_scheduler.add_task(task_2, 20)
list(thread_pool_scheduler.results())
# Executed with result 20
# Executed with result 60
# => [20, 60]
```

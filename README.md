This project (**sqsi**, short for SQS iterable, pronounced "squeezy") is a way to access SQS queues as async iterators
in Python, because I am tired of writing custom SQS logic every time I just want to read stuff out of a queue.

The basic operation of the library is like this:

```python
async with sqsi.QueueIterator(uri=...) as queue:
    async for item in queue:
        ... # process the item

        # If no exception is thrown, the item will be automatically deleted
        # from the queue when the next item is requested.
```

If you want more fine-grained control over when items are deleted from the queue, you can manually delete the items. For
convenience, the `mark_complete` method can accept any number of handles as positional arguments.

```python
async with sqsi.QueueIterator(uri=..., deletion_mode='handle') as queue:
    async for item, receipt_handle in queue:
        ... # process the item

        await queue.mark_complete(receipt_handle)
```

Or, to avoid having to pass around the `queue` object to other classes, you can ask for a callback method instead
(though make sure you are still somehow within the async context where `queue` is valid):

```python
async with sqsi.QueueIterator(uri=..., deletion_mode='callback') as queue:
    async for item, complete in queue:
        ... # process the item

        await complete()
```

If you need to process messages in chunks of a certain size, a builtin method is provided to generate those chunks.

```python
async with sqsi.QueueIterator(uri=..., deletion_mode='callback') as queue:
    async for chunk in queue.chunks(size=10):
        for item, complete in chunk:
            ... # process each item in the chunk
            await complete()
```

Note that a deletion mode of `callback` or `handle` is recommended when processing chunks, as otherwise an entire chunk
is marked completed at the same time when the chunk is finished, which can lead to counter-intuitive behavior.

If a timeout is passed to `chunks`, it will wait until the timeout or until it has a chunk of sufficient size, whichever
comes first.

```python
async with sqsi.QueueIterator(uri=..., deletion_mode='callback') as queue:
    async for chunk in queue.chunks(size=10, timeout=30):
        for item, complete in chunk:
            ... # process each item in the chunk
            await complete()
```

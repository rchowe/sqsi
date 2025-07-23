import asyncio
import logging
import itertools
from typing import Literal, Optional
from aiobotocore.session import AioSession
from collections.abc import AsyncIterator
from types_aiobotocore_sqs import SQSClient
from types_aiobotocore_sqs.type_defs import MessageTypeDef


logger = logging.getLogger(__name__)


class QueueIterator (AsyncIterator[str]):
    """
    An iterable over an SQS queue.
    """

    def __init__(
        self,
        uri: str,
        session: Optional[AioSession] = None,
        buffer_size: Optional[int] = None,
        wait_time: Optional[int] = None,
        visibility_timeout: Optional[int] = None,
        deletion_mode: Optional[Literal['auto', 'handle', 'callback']] = None,
        stop_on_empty_response: Optional[bool] = None,
    ):
        self.queue_uri = uri
        self._buffer: asyncio.Queue[MessageTypeDef] = asyncio.Queue()
        self._session = session or AioSession(None)
        self._sqs_client: Optional[SQSClient] = None
        self._wait_time = wait_time
        self._visibility_timeout = visibility_timeout
        self._deletion_mode = deletion_mode or 'auto'
        self._previous_receipt_handle: Optional[str] = None
        self._stop_on_empty_response: Optional[bool] = stop_on_empty_response or False

        try:
            self._buffer_size = int(buffer_size or 1)
            if self._buffer_size <= 0 or self._buffer_size > 10:
                raise ValueError("Buffer size must be an integer between 0 and 10")
        except (TypeError, ValueError):
            raise ValueError("Invalid value for `buffer_size` in sqsi.QueueIterator constructor()")

    async def __aenter__(self):
        self._buffer = asyncio.Queue()
        self._sqs_client = await self._session.create_client('sqs').__aenter__()
        self._previous_receipt_handle = None

        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._sqs_client:
            await self._sqs_client.__aexit__(exc_type, exc_val, exc_tb)
            self._sqs_client = None
        self._previous_receipt_handle = None

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self._deletion_mode == "auto" and self._previous_receipt_handle:
            await self._sqs_client.delete_message(
                QueueUrl=self.queue_uri,
                ReceiptHandle=self._previous_receipt_handle,
            )
            self._previous_receipt_handle = None
        
        return await self.get_next_item(self._deletion_mode)

    async def get_next_item(self, deletion_mode: Literal['auto', 'handle', 'callback']):
        while self._buffer.empty():
            response = await self._sqs_client.receive_message(
                QueueUrl=self.queue_uri,
                MaxNumberOfMessages=self._buffer_size,
                **{k: v for k, v in dict(
                    WaitTimeSeconds=self._wait_time,
                    VisibilityTimeout=self._visibility_timeout,
                ).items() if v is not None},
            )

            if not (messages := response.get('Messages', [])):
                if self._stop_on_empty_response:
                    raise StopAsyncIteration
                else:
                    continue
            
            for message in messages:
                self._buffer.put_nowait(message)

        message = self._buffer.get_nowait()
        
        body = message.get('Body')
        receipt_handle = message.get('ReceiptHandle')

        if self._deletion_mode == "callback":
            async def callback():
                await self._sqs_client.delete_message(
                    QueueUrl=self.queue_uri,
                    ReceiptHandle=receipt_handle,
                )
            return body, callback
        elif self._deletion_mode == "handle":
            return body, receipt_handle
        elif self._deletion_mode == "auto":
            self._previous_receipt_handle = receipt_handle
            return body

        raise ValueError(f"Invalid deletion mode `{self._deletion_mode}`")

    async def mark_complete(self, *receipt_handles):
        """
        Mark a message as successfully processed, deleting it from the queue.

        Note that you only need to use `mark_complete()` on queues where the deletion mode is `handle`. Other queue
        types will have different deletion behaviors.
        """
        if self._deletion_mode != "handle":
            logger.warning("mark_complete() was called on a queue iterator with a deletion mode other than `handle`.")

        iterator = iter(receipt_handles)
        while handle_chunk := list(itertools.islice(iterator, 10)):
            await self._sqs_client.delete_message_batch(
                QueueUrl=self.queue_uri,
                Entries=[
                    {
                        'Id': str(index),
                        'ReceiptHandle': receipt_handle
                    }
                    for index, receipt_handle in enumerate(handle_chunk)
                ]
            )
    
    async def chunks(self, size: int, timeout: Optional[float] = None):
        """
        Break the iterator into chunks of maximum size `size`.

        Note that it is strongly recommended to use this on an iterator with `deletion_mode` set to `handle` or
        `callback`. Using this on an iterator set to `auto` will only delete items from the queue if the whole chunk
        completes successfully, which may lead to counter-intuitive behavior.

        ## Usage

        ### Callback Deletion

        ```python
        async with sqsi.QueueIterator(uri=..., deletion_mode='callback') as queue:
            async for chunk in queue.chunks(size=10):
                for item, complete in chunk:
                    ... # process each item in the chunk
                    await complete()
        ```

        ### Handle Deletion

        ```python
        async with sqsi.QueueIterator(uri=..., deletion_mode='handle') as queue:
            async for chunk in queue.chunks(size=10):
                for item, receipt_handle in chunk:
                    ... # process each item in the chunk
                    await queue.mark_complete(receipt_handle)
        ```

        ### Automatic Deletion

        ```python
        async with sqsi.QueueIterator(uri=...) as queue:
            async for chunk in queue.chunks(size=10):
                ... # process each item in a chunk.
        ```
        """
        
        finished = False
        while not finished:
            start_time = asyncio.get_event_loop().time()
            results = []
            
            for _ in range(size):
                wait_time = None
                if timeout is not None:
                    elapsed_time = asyncio.get_event_loop().time() - start_time
                    wait_time = timeout - elapsed_time
                    if wait_time <= 0.0:
                        break

                try:
                    results.append(await asyncio.wait_for(anext(self), wait_time))
                except TimeoutError:
                    break
                except StopAsyncIteration:
                    finished = True
            
            if results:
                yield results
        
        raise StopAsyncIteration

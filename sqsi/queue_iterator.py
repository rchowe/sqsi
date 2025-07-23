import asyncio
import asyncio.exceptions
import logging
import itertools
from typing import Any, Callable, Literal, Optional, TypeVar
from aiobotocore.session import AioSession
from collections.abc import AsyncIterator
from types_aiobotocore_sqs import SQSClient
from types_aiobotocore_sqs.type_defs import ReceiveMessageRequestTypeDef, MessageTypeDef


logger = logging.getLogger(__name__)


T = TypeVar("T", default=str)


class QueueIterator[T] (AsyncIterator[T]):
    """
    An iterable over an SQS queue.
    """

    def __init__(
        self,
        uri: str,
        session: Optional[AioSession] = None,
        buffer_size: Optional[int] = None,
        wait_time: Optional[int] = None,
        deletion_mode: Literal['auto', 'handle', 'callback'] = 'auto',
        stop_on_empty_response: bool = False,
        receive_message_options: Optional[ReceiveMessageRequestTypeDef] = None,
        transformer: Optional[Callable[[str], T]] = None,
        transformer_exceptions: Literal['skip', 'skip-delete' 'return-none', 'return-exception', 'throw-exception'] = 'throw-exception',
    ):
        self.queue_uri = uri
        self._buffer: asyncio.Queue[MessageTypeDef] = asyncio.Queue()
        self._session = session or AioSession(None)
        self._sqs_client: Optional[SQSClient] = None
        self._wait_time = wait_time
        self._deletion_mode = deletion_mode
        self._previous_receipt_handle: Optional[str] = None
        self._stop_on_empty_response: Optional[bool] = stop_on_empty_response or False
        self._transformer = transformer
        self._transformer_exceptions = transformer_exceptions

        try:
            self._buffer_size = int(buffer_size or 1)
            if self._buffer_size <= 0 or self._buffer_size > 10:
                raise ValueError("Buffer size must be an integer between 0 and 10")
        except (TypeError, ValueError):
            raise ValueError("Invalid value for `buffer_size` in sqsi.QueueIterator constructor()")

        self._receive_message_options: ReceiveMessageRequestTypeDef = {
            'QueueUrl': self.queue_uri,
            'MaxNumberOfMessages': self._buffer_size,
            'WaitTimeSeconds': self._wait_time,
            **(receive_message_options or {}),
        }

    async def __aenter__(self):
        self._buffer = asyncio.Queue()
        self._sqs_client = await self._session.create_client('sqs').__aenter__()
        self._previous_receipt_handle = None

        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._sqs_client:
            try:
                await self._sqs_client.__aexit__(exc_type, exc_val, exc_tb)
            except asyncio.exceptions.CancelledError:
                pass
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
        while True:
            while self._buffer.empty():
                response = await self._sqs_client.receive_message(**{
                    k: v
                    for k, v in self._receive_message_options.items()
                    if v is not None
                })

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

            item = None
            if self._transformer:
                try:
                    item = self._transformer(body)
                except Exception as e:
                    if self._transformer_exceptions == 'skip':
                        continue
                    if self._transformer_exceptions == 'skip-delete':
                        await self._mark_complete(receipt_handle)
                        continue
                    elif self._transformer_exceptions == 'return-exception':
                        item = e
                    elif self._transformer_exceptions == 'return-none':
                        item = None
                    else:
                        raise
            else:
                item = body

            if deletion_mode == "callback":
                async def callback():
                    await self._mark_complete(receipt_handle)
                return item, callback
            elif deletion_mode == "handle":
                return item, receipt_handle
            elif deletion_mode == "auto":
                self._previous_receipt_handle = receipt_handle
                return item

            raise ValueError(f"Invalid deletion mode `{self._deletion_mode}`")

    async def mark_complete(self, *receipt_handles):
        """
        Mark a message as successfully processed, deleting it from the queue.

        Note that you only need to use `mark_complete()` on queues where the deletion mode is `handle`. Other queue
        types will have different deletion behaviors.
        """
        if self._deletion_mode != "handle":
            logger.warning("mark_complete() was called on a queue iterator with a deletion mode other than `handle`.")
        
        self._mark_complete(*receipt_handles)

    async def _mark_complete(self, *receipt_handles):
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
        last_batch_deletion_handles = None
        while not finished:
            start_time = asyncio.get_event_loop().time()
            results = []

            if self._deletion_mode == 'auto' and last_batch_deletion_handles:
                await self._mark_complete(*last_batch_deletion_handles)
            
            for _ in range(size):
                wait_time = None
                if timeout is not None:
                    elapsed_time = asyncio.get_event_loop().time() - start_time
                    wait_time = timeout - elapsed_time
                    if wait_time <= 0.0:
                        break

                deletion_mode = self._deletion_mode
                if self._deletion_mode == 'auto':
                    deletion_mode = 'handle'

                try:
                    results.append(await asyncio.wait_for(self.get_next_item(deletion_mode), wait_time))
                except TimeoutError:
                    break
                except StopAsyncIteration:
                    finished = True
            
            if results:
                if self._deletion_mode == 'auto':
                    yield [item for item, _ in results]
                    last_batch_deletion_handles = [handle for _, handle in results]
                else:
                    yield results

        if self._deletion_mode == 'auto' and last_batch_deletion_handles:
            await self._mark_complete(*last_batch_deletion_handles)

        raise StopAsyncIteration

# -*- coding: utf-8 -*-
# pylint: skip-file
import asyncio
import logging
import os

from collections import Counter
from itertools import islice

import aiohttp
import ujson
from funcy.colls import get_in


from progress.bar import Bar

#asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
loop = asyncio.get_event_loop()
#loop.set_debug(True)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


CORRECT_BATCH_TEST_RESPONSE= [
    {"id": 1, "result": {"previous": "000000b0c668dad57f55172da54899754aeba74b", "timestamp": "2016-03-24T16:14:21",
                         "witness": "initminer", "transaction_merkle_root": "0000000000000000000000000000000000000000",
                         "extensions": [],
                         "witness_signature": "2036fd4ff7838ba32d6d27637576e1b1e82fd2858ac97e6e65b7451275218cbd2b64411b0a5d74edbde790c17ef704b8ce5d9de268cb43783b499284c77f7d9f5e",
                         "transactions": [], "block_id": "000000b13707dfaad7c2452294d4cfa7c2098db4",
                         "signing_key": "STM8GC13uCZbP44HzMLV6zPZGwVQ8Nt4Kji8PapsPiNq1BK153XTX",
                         "transaction_ids": []}},
    {"id": 2, "result": {"previous": "000000b0c668dad57f55172da54899754aeba74b", "timestamp": "2016-03-24T16:14:21",
                         "witness": "initminer", "transaction_merkle_root": "0000000000000000000000000000000000000000",
                         "extensions": [],
                         "witness_signature": "2036fd4ff7838ba32d6d27637576e1b1e82fd2858ac97e6e65b7451275218cbd2b64411b0a5d74edbde790c17ef704b8ce5d9de268cb43783b499284c77f7d9f5e",
                         "transactions": [], "block_id": "000000b13707dfaad7c2452294d4cfa7c2098db4",
                         "signing_key": "STM8GC13uCZbP44HzMLV6zPZGwVQ8Nt4Kji8PapsPiNq1BK153XTX", "transaction_ids": []}}
]


NO_BATCH_SUPPORT_RESPONSE = '7 bad_cast_exception: Bad Cast'

CORRECT_GZIP_TEST_RESPONSE= {
    "id": 1, "result": {"previous": "000000b0c668dad57f55172da54899754aeba74b", "timestamp": "2016-03-24T16:14:21",
                         "witness": "initminer", "transaction_merkle_root": "0000000000000000000000000000000000000000",
                         "extensions": [],
                         "witness_signature": "2036fd4ff7838ba32d6d27637576e1b1e82fd2858ac97e6e65b7451275218cbd2b64411b0a5d74edbde790c17ef704b8ce5d9de268cb43783b499284c77f7d9f5e",
                         "transactions": [], "block_id": "000000b13707dfaad7c2452294d4cfa7c2098db4",
                         "signing_key": "STM8GC13uCZbP44HzMLV6zPZGwVQ8Nt4Kji8PapsPiNq1BK153XTX",
                         "transaction_ids": []}
}

NON_JSON_ERROR_MESSAGE = 'Attempt to decode JSON with unexpected mimetype: text/html; charset=utf-8'

GET_BLOCK_RESULT_KEYS = {"previous",
        "timestamp",
        "witness",
        "transaction_merkle_root",
        "extensions",
        "witness_signature",
        "transactions" ,
        "block_id",
        "signing_key",
        "transaction_ids"}


'''

hivemind steemd calls

- get_blocks
- gdgp
- get_accounts
- get_content

'''

class NonJsonResponseError(Exception):
    pass

class InvalidResponseBlockNum(Exception):
    pass

class InvalidResponseObjectKeys(Exception):
    pass

class JsonRpcErrorResponse(Exception):
    pass

class RateBar(Bar):
    suffix = '%(index)d (%(rate)d/sec) time remaining: %(eta_td)s'

    @property
    def rate(self):
        if not self.elapsed:
            return 0
        return self.index/self.elapsed

def chunkify(iterable, chunksize=3000):
    i = 0
    chunk = []
    for item in iterable:
        chunk.append(item)
        i += 1
        if i == chunksize:
            yield chunk
            i = 0
            chunk = []
    if chunk:
        yield chunk


class AsyncClient(object):
    def __init__(self, *, url=None, **kwargs):
        self.url = url or os.environ.get('STEEMD_HTTP_URL', 'https://steemd.steemitdev.com')
        self.kwargs = kwargs
        self.session = kwargs.get('session', None)
        self.connector = get_in(kwargs, ['session','connector'])

        self.supports_batch = kwargs.get('supports_batch',True)
        self.supports_gzip = kwargs.get('supports_gzip', True)

        if self.supports_batch is None or self.supports_gzip is None:
            self.determine_endpoint_capabilities()

        if not self.connector:
            self.connector = self._new_connector()
        if not self.session:
            self.session = self._new_session()

        self.retry_limit = 4

        self._batch_request_size = self.kwargs.get('batch_request_size', 50)
        self._concurrent_tasks_limit = self.kwargs.get('concurrent_tasks_limit', 10)

        self.verify_responses = kwargs.get('verify_responses', True)


        self._batch_request_count = 0
        self._request_count = 0
        self._errors = Counter()

    def _new_connector(self, connector_kwargs=None):
        connector_kwargs = connector_kwargs or self._connector_kwargs
        return aiohttp.TCPConnector(**connector_kwargs)

    def _new_session(self, session_kwargs=None):
        session_kwargs = session_kwargs or self._session_kwargs
        return aiohttp.ClientSession(**session_kwargs)

    async def fetch_async(self, request_data):
        kwargs = {'json':request_data}
        if self.supports_gzip:
            kwargs['compress'] = 'gzip'
        if isinstance(request_data, list):
            self._batch_request_count += 1
            self._request_count += len(request_data)
            kwargs['headers'] = {'x-jussi-request-id': f'{request_data[0]["id"]}-{request_data[-1]["id"]}'}
        else:
            kwargs['headers'] = {'x-jussi-request-id': f'{request_data["id"]}'}
        attempts = 0
        while attempts <= self.retry_limit:
            attempts += 1
            async with self.session.post(self.url, **kwargs) as response:
                try:
                    response.raise_for_status()
                    if response.headers['Content-Type'] != 'application/json':
                        self._errors['content-type'] += 1
                        continue
                    response_data = await response.json()
                    if self.verify_responses:
                        verify(request_data, response, response_data, _raise=True)
                    return response_data
                except aiohttp.ClientResponseError as e:
                    logger.info(f'Client Response HTTP Status error:{e}')
                except Exception as e:
                    logger.exception(e)
                    content = await response.read()
                    logger.error(f'{kwargs["headers"]} {response.status}\n {response.raw_headers}\n {response.headers}\n {response._get_encoding()}\n {content}\n\n')


    async def get_blocks(self, block_nums):
        requests = ({'jsonrpc':'2.0','id':block_num, 'method':'get_block','params':[block_num]} for block_num in block_nums)
        batched_requests = chunkify(requests, self.batch_request_size)
        coros = (self.fetch_async(batch) for batch in batched_requests)
        first_coros = islice(coros,0,self.concurrent_tasks_limit)
        futures = [asyncio.ensure_future(c) for c in first_coros]

        logger.debug(f'inital futures:{len(futures)}')

        while futures:
            await asyncio.sleep(0)
            for f in futures:
                try:
                    if f.done():
                        result = f.result()
                        futures.remove(f)
                        logger.debug(f'futures:{len(futures)}')
                        try:
                            futures.append(asyncio.ensure_future(next(coros)))
                        except StopIteration as e:
                            logger.debug('StopIteration')
                        yield self.strip_jsonrpc_response_envelope(result)
                except KeyboardInterrupt:
                    for f in futures:
                        f.cancel()
                    tasks = asyncio.Task.all_tasks()
                    for t in tasks:
                        t.cancel()

                except Exception as e:
                    logger.error(e)


    async def test_batch_support(self, url):
        batch_request = [{"id":1,"jsonrpc":"2.0","method":"get_block","params":[1]},{"id":2,"jsonrpc":"2.0","method":"get_block","params":[1]}]
        try:
            async with self.session.post(self.url, json=batch_request) as response:
                response.raise_for_status()
                response_data = await response.json()
            if response_data.startswith(NO_BATCH_SUPPORT_RESPONSE):
                return False
            response_json = ujson.loads(response_data)
            assert len(response_json) == 2
            assert isinstance(response_json, list)
            for i,result in enumerate(response_json):
                print(result)
                print(CORRECT_BATCH_TEST_RESPONSE[i])
                assert result == CORRECT_BATCH_TEST_RESPONSE[i]
        except Exception as e:
            logger.debug(f'test_batch_support error{e}')
        return False

    async def test_gzip_support(self, url):
        request = {"id":1,"jsonrpc":"2.0","method":"get_block","params":[1]}
        try:
            async with self.session.post(self.url, json=request, compress='gzip') as response:
                response_json = await response.json()
                response.raise_for_status()
            assert response_json == CORRECT_GZIP_TEST_RESPONSE
        except Exception as e:
            logger.debug(f'test_gzip_support error{e}')
        return False

    async def strip_jsonrpc_response_envelope(self, response):
        return ujson.dumps(response['result'])

    def determine_endpoint_capabilities(self):
        loop = asyncio.get_event_loop()
        url = self.url
        batch = loop.run_until_complete(self.test_batch_support(url))
        gzip = loop.run_until_complete(self.test_gzip_support(url))


    @property
    def _session_kwargs(self):
        session_kwargs = self.kwargs.get('session_kwargs', {})
        session_kwargs['skip_auto_headers'] = session_kwargs.get('skip_auto_headers', ['User-Agent'])
        session_kwargs['json_serialize'] = session_kwargs.get('json_serialize', ujson.dumps)
        session_kwargs['headers'] = session_kwargs.get('headers', {'Content-Type': 'application/json'})
        session_kwargs['connector'] = session_kwargs.get('connector', None)
        return session_kwargs

    @property
    def _connector_kwargs(self):
        connector_kwargs = self.kwargs.get('connector_kwargs', {})
        connector_kwargs['keepalive_timeout'] = connector_kwargs.get('keepalive_timeout', 60)
        connector_kwargs['limit'] = connector_kwargs.get('limit', 100)
        return connector_kwargs

    @property
    def concurrent_connections(self):
        """number of tcp connections to steemd"""
        return self.connector.limit

    @property
    def batch_request_size(self):
        """number of individual jsonrpc requests to combine into a jsonrpc batch request"""
        return self._batch_request_size

    @property
    def concurrent_tasks_limit(self):
        """number of jsonrpc batch requests tasks to submit to event loop at any one time"""
        return self._concurrent_tasks_limit

    def close(self):
        self.session.close()
        for task in asyncio.Task.all_tasks():
            task.cancel()


def block_num_from_id(block_hash: str) -> int:
    """return the first 4 bytes (8 hex digits) of the block ID (the block_num)
    """
    return int(str(block_hash)[:8], base=16)

def verify_get_block_response(request_id, response, response_data, _raise=False):
    try:
        if 'error' in response_data:
            raise JsonRpcErrorResponse(response_data)
        response_id = response_data['id']
        block_num = block_num_from_id(response_data['result']['block_id'])
        response_keys = set(response_data['result'].keys())
        if response_id != block_num:
            raise InvalidResponseBlockNum(f'{request_id} {response_id} {block_num}')
        if response_keys != GET_BLOCK_RESULT_KEYS:
            raise InvalidResponseObjectKeys(f'keys:{response_data["result"].keys()}')
        return True
    except KeyError as e:
        logger.exception(f'keys:{response_data["result"].keys()} {e}')
    except Exception as e:
        logger.error(f'error validating response:{e}')
    return False

def verify(request_data, response, response_data, _raise=True):
    if isinstance(response_data, list):
        request_ids = {r['id'] for r in request_data}
        for i,data in enumerate(response_data):
            verify_get_block_response(request_ids, response, data, _raise=_raise)
    else:
        verify_get_block_response({request_data['id']}, response, response_data, _raise=_raise)


async def get_blocks(args):
    block_nums = range(args.start_block, args.end_block)
    url = args.url
    batch_request_size = args.batch_request_size
    concurrent_tasks_limit = args.concurrent_tasks_limit
    concurrent_connections = args.concurrent_connections

    client = AsyncClient(url=url,
                         batch_request_size=batch_request_size,
                         concurrent_tasks_limit=concurrent_tasks_limit,
                         connector_kwargs={'limit': concurrent_connections})

    bar = RateBar('Fetching blocks', max=len(block_nums))

    try:
        async for result in client.get_blocks(block_nums):
            if result:
                bar.next(n=len(result))
                print(ujson.dumps(result))
            else:
                logger.error('encountered missing result')
    except KeyboardInterrupt:
        tasks = asyncio.Task.all_tasks()
        for t in tasks:
            t.cancel()
    except Exception as e:
        logger.error(e)
    finally:
        bar.finish()
        client.close()


if __name__ == '__main__':
    import sys
    import argparse
    logging.basicConfig(level=logging.DEBUG)
    logger = logging.getLogger('async_http_client_main')

    parser = argparse.ArgumentParser('jussi client')

    subparsers = parser.add_subparsers()

    parser.add_argument('--url', type=str, default='https://api.steemitdev.com')
    parser.add_argument('--start_block', type=int, default=1)
    parser.add_argument('--end_block', type=int, default=15_000_000)
    parser.add_argument('--batch_request_size', type=int, default=100)
    parser.add_argument('--concurrent_tasks_limit', type=int, default=5)
    parser.add_argument('--concurrent_connections', type=int, default=5)
    parser.add_argument('--print', type=bool, default=False)

    parser_get_blocks = subparsers.add_parser('get-blocks')
    parser_get_blocks.set_defaults(func=get_blocks)

    args = parser.parse_args()
    func = getattr(args, 'func', None)
    if not func:
        parser.print_help()
        sys.exit()
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(args.func(args))
    except KeyboardInterrupt:
        for t in asyncio.Task.all_tasks():
            t.cancel()
        loop.shutdown_asyncgens()
    finally:
        for t in asyncio.Task.all_tasks():
            t.cancel()
        loop.shutdown_asyncgens()

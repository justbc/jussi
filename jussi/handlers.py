# -*- coding: utf-8 -*-

import asyncio
import datetime
import functools
import logging

import ujson
from sanic import response

from .typedefs import BatchJsonRpcRequest
from .typedefs import BatchJsonRpcResponse
from .typedefs import HTTPRequest
from .typedefs import HTTPResponse
from .typedefs import JsonRpcRequest
from .typedefs import SingleJsonRpcRequest
from .typedefs import SingleJsonRpcResponse
from .utils import is_batch_jsonrpc
from .utils import upstream_url_from_jsonrpc_request
from .validators import validate_response

logger = logging.getLogger(__name__)


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

async def _block_streamer(sanic_http_request, batched_requests, response):
    for batch in batched_requests:
        batch_response = await dispatch_batch(sanic_http_request, batch)
        for jrpc_response in batch_response:
            block = jrpc_response['result']
            response.write(ujson.dumps(block).encode())


async def jussi_get_blocks(sanic_http_request: HTTPRequest) -> response.StreamingHTTPResponse:
    # retreive parsed jsonrpc_requests after request middleware processing
    app = sanic_http_request.app
    jsonrpc_request = sanic_http_request.json  # type: JsonRpcRequest
    start_block = jsonrpc_request['params'].get('start_block',1)
    end_block = jsonrpc_request['params'].get('end_block', 15_000_000)
    block_nums = range(start_block, end_block)
    requests = ({'id':block_num,'jsonrpc':'2.0','method':'get_block','params':[block_num]} for block_num in block_nums)
    batched_requests = chunkify(requests, 10)
    block_streamer = functools.partial(_block_streamer, sanic_http_request, batched_requests)
    return response.stream(block_streamer, content_type='application/json')


# path /
async def handle_jsonrpc(sanic_http_request: HTTPRequest) -> HTTPResponse:
    # retreive parsed jsonrpc_requests after request middleware processing
    jsonrpc_requests = sanic_http_request.json  # type: JsonRpcRequest

    # make upstream requests

    if is_batch_jsonrpc(sanic_http_request=sanic_http_request):
        jsonrpc_response = await dispatch_batch(sanic_http_request,
                                                jsonrpc_requests)

    elif sanic_http_request.json['method'] == 'jussi.get_blocks':
        return await jussi_get_blocks(sanic_http_request)

    else:
        jsonrpc_response = await dispatch_single(sanic_http_request,
                                                 jsonrpc_requests)
    return response.json(jsonrpc_response)



# pylint: disable=unused-argument
async def healthcheck(sanic_http_request: HTTPRequest) -> HTTPResponse:
    return response.json({
        'status': 'OK',
        'datetime': datetime.datetime.utcnow().isoformat(),
        'source_commit': sanic_http_request.app.config.args.source_commit
    })


@validate_response
async def fetch_ws(sanic_http_request: HTTPRequest,
                   jsonrpc_request: SingleJsonRpcRequest
                   ) -> SingleJsonRpcResponse:
    pool = sanic_http_request.app.config.websocket_pool
    conn = await pool.acquire()
    try:
        await conn.send(ujson.dumps(jsonrpc_request).encode())
        return ujson.loads(await conn.recv())
    finally:
        pool.release(conn)

# pylint: enable=unused-argument


@validate_response
async def fetch_http(sanic_http_request: HTTPRequest=None,
                     jsonrpc_request: SingleJsonRpcRequest=None,
                     url: str=None) -> SingleJsonRpcResponse:
    session = sanic_http_request.app.config.aiohttp['session']
    async with session.post(url, json=jsonrpc_request) as resp:
        json_response = await resp.json()
    return json_response


async def dispatch_single(sanic_http_request: HTTPRequest,
                          jsonrpc_request: SingleJsonRpcRequest) -> SingleJsonRpcResponse:
    url = upstream_url_from_jsonrpc_request(
        sanic_http_request.app.config.upstream_urls, jsonrpc_request)
    # pylint: disable=unexpected-keyword-arg
    if url.startswith('ws'):
        json_response = await fetch_ws(
            sanic_http_request,
            jsonrpc_request)
    else:
        json_response = await fetch_http(
            sanic_http_request,
            jsonrpc_request,
            url)
    return json_response


async def dispatch_batch(sanic_http_request: HTTPRequest,
                         jsonrpc_requests: BatchJsonRpcRequest
                         ) -> BatchJsonRpcResponse:
    requests = [dispatch_single(sanic_http_request, request)
                for request in jsonrpc_requests]
    return await asyncio.gather(*requests)

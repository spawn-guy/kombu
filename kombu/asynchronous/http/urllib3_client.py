"""HTTP Client using urllib3."""

from __future__ import annotations

from collections import deque
from io import BytesIO

import urllib3
from urllib3.exceptions import HTTPError
from urllib3.util import Url, make_headers

from kombu.asynchronous.hub import Hub, get_event_loop
from kombu.exceptions import HttpError

from .base import BaseClient

__all__ = ('Urllib3Client',)

DEFAULT_USER_AGENT = 'Mozilla/5.0 (compatible; urllib3)'
EXTRA_METHODS = frozenset(['DELETE', 'OPTIONS', 'PATCH'])


class Urllib3Client(BaseClient):
    """Urllib3Client HTTP Client (using urllib3)."""

    def __init__(self, hub: Hub | None = None, max_clients: int = 10):
        hub = hub or get_event_loop()
        super().__init__(hub)
        self.max_clients = max_clients
        self._pending = deque()
        self._pools = {}
        self._timeout_check_tref = self.hub.call_repeatedly(
            1.0, self._timeout_check,
        )

    def close(self):
        """Close the client and all connection pools."""
        self._timeout_check_tref.cancel()
        for pool in self._pools.values():
            pool.close()
        self._pools.clear()

    def add_request(self, request):
        """Add a request to the pending queue."""
        self._pending.append(request)
        self._process_queue()
        return request

    def _get_pool(self, request):
        """Get or create a connection pool for the request."""
        # Prepare connection kwargs
        conn_kwargs = {}

        # Network Interface
        if request.network_interface:
            conn_kwargs['source_address'] = (request.network_interface, 0)

        # SSL Verification
        conn_kwargs['cert_reqs'] = 'CERT_REQUIRED' if request.validate_cert else 'CERT_NONE'

        # CA Certificates
        if request.ca_certs is not None:
            conn_kwargs['ca_certs'] = request.ca_certs
        elif request.validate_cert is True:
            try:
                from certifi import where
                conn_kwargs['ca_certs'] = where()
            except ImportError:
                pass

        # Client Certificates
        if request.client_cert is not None:
            conn_kwargs['cert_file'] = request.client_cert
        if request.client_key is not None:
            conn_kwargs['key_file'] = request.client_key

        # Handle proxy configuration
        if request.proxy_host:
            conn_kwargs['_proxy'] = Url(
                scheme=None,
                host=request.proxy_host,
                port=request.proxy_port,
            ).url

            if request.proxy_username:
                conn_kwargs['_proxy_headers'] = make_headers(
                    proxy_basic_auth=f"{request.proxy_username}:{request.proxy_password or ''}"
                )

        pool = urllib3.connection_from_url(request.url, **conn_kwargs)
        return pool

        # # Create pool key
        # key = (request.url, request.proxy_host, request.proxy_port)

        # if key in self._pools:
        #     return self._pools[key]
        #
        # # Create appropriate pool based on proxy settings
        # if proxy_url:
        #     pool = urllib3.ProxyManager(
        #         proxy_url=proxy_url,
        #         proxy_headers=proxy_headers,
        #         maxsize=self.max_clients,
        #         block=True,
        #         **{k: v for k, v in conn_kwargs.items() if not k.startswith('_')}
        #     )
        # else:
        #     pool = urllib3.PoolManager(
        #         maxsize=self.max_clients,
        #         block=True,
        #         **conn_kwargs
        #     )
        #
        # self._pools[key] = pool
        # return pool

    def _timeout_check(self):
        """Check for timeouts and process pending requests."""
        self._process_pending_requests()

    def _process_pending_requests(self):
        """Process all pending requests."""
        while self._pending:
            request = self._pending.popleft()
            self._process_request(request)

    def _process_request(self, request):
        """Process a single request using urllib3."""
        # Prepare headers
        headers = dict(request.headers)
        headers.update(
            make_headers(
                user_agent=request.user_agent or DEFAULT_USER_AGENT,
                accept_encoding=request.use_gzip,
            )
        )

        # Authentication
        if request.auth_username is not None:
            headers.update(
                make_headers(
                    basic_auth=f"{request.auth_username}:{request.auth_password or ''}"
                )
            )

        # Process request body
        body = None
        if request.body:
            body = request.body if isinstance(request.body, bytes) else request.body.encode('utf-8')

        # Make the request using urllib3
        try:
            pool = self._get_pool(request)
            response = pool.request(
                request.method,
                request.url,
                headers=headers,
                body=body,
                preload_content=False,
                redirect=request.follow_redirects,
                retries=False,  # Handle redirects manually to match pycurl behavior
            )

            buffer = BytesIO(response.data)
            response_obj = self.Response(
                request=request,
                code=response.status,
                headers=response.headers,
                buffer=buffer,
                effective_url=response.geturl(),
                error=None
            )
            response.release_conn()
        except HTTPError as e:
            response_obj = self.Response(
                request=request,
                code=599,
                headers={},
                buffer=None,
                effective_url=None,
                error=HttpError(599, str(e))
            )

        request.on_ready(response_obj)

    def _process_queue(self):
        """Process the request queue."""
        self._process_pending_requests()

    def on_readable(self, fd):
        """Compatibility method for the event loop."""
        pass

    def on_writable(self, fd):
        """Compatibility method for the event loop."""
        pass

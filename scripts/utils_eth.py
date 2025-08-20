# utils_eth.py
import os
import asyncio
import aiohttp
import random
import json
from contextlib import asynccontextmanager
from typing import Iterator, List, Union, Tuple

DEFAULT_RPC = os.getenv("ETH_RPC_URL", "https://cloudflare-eth.com")

class RpcPool:
    """ Lightweight rotating RPC pool with retries and exponential backoff.

    `post` accepts a dict or a list (batch). Returns (ok: bool, data: dict|list|{'error':...})
    """
    def __init__(self, urls: Union[str, List[str]] = None, concurrency: int = 12, timeout: int = 30, max_retries: int = 6):
        urls = urls or os.getenv("ETH_RPC_URL", DEFAULT_RPC)
        if isinstance(urls, str):
            self.urls = [u.strip() for u in urls.split(",") if u.strip()]
        else:
            self.urls = list(urls or [])
        if not self.urls:
            raise ValueError("No RPC endpoints configured")
        self._rr = 0
        self._session: aiohttp.ClientSession | None = None
        self.sem = asyncio.Semaphore(max(1, int(concurrency)))
        self.timeout = timeout
        self.max_retries = max_retries

    def _pick(self) -> str:
        url = self.urls[self._rr % len(self.urls)]
        self._rr += 1
        return url

    @asynccontextmanager
    async def _client(self):
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(raise_for_status=False)
        try:
            yield self._session
        finally:
            # do not close here; allow reuse. Use close() when finished.
            pass

    async def post(self, payload, timeout: int | None = None) -> Tuple[bool, object]:
        """ Accepts JSON-RPC single payload (dict) or batch payload (list).
            Returns (ok, parsed_json_or_error).
        """
        timeout = timeout or self.timeout
        async with self.sem:
            last_err = None
            for attempt in range(1, self.max_retries + 1):
                url = self._pick()
                try:
                    async with self._client() as client:
                        # exponential backoff base (seconds)
                        async with client.post(url, json=payload, timeout=timeout) as resp:
                            text = await resp.text()
                            if resp.status == 429:
                                last_err = f"http 429: {text[:200]}"
                                # on 429, backoff longer
                                backoff = min(30, 0.5 * (2 ** attempt))
                                await asyncio.sleep(backoff + random.random() * 0.5)
                                continue
                            if resp.status >= 500:
                                last_err = f"http {resp.status}: {text[:200]}"
                                await asyncio.sleep(min(30, 0.5 * (2 ** attempt)) + random.random() * 0.5)
                                continue
                            if resp.status != 200:
                                last_err = f"http {resp.status}: {text[:200]}"
                                await asyncio.sleep(min(10, 0.3 * attempt) + random.random() * 0.2)
                                continue
                            try:
                                data = json.loads(text)
                            except Exception:
                                return False, {"error": "invalid_json", "text": text[:1000]}
                            # If JSON-RPC returns top-level {"error":...}
                            if isinstance(data, dict) and "error" in data:
                                last_err = f"rpc_error: {data['error']}"
                                await asyncio.sleep(min(10, 0.4 * attempt))
                                continue
                            return True, data
                except Exception as e:
                    last_err = str(e)
                    await asyncio.sleep(min(10, 0.5 * attempt) + random.random() * 0.2)
            return False, {"error": last_err}

    async def close(self):
        if self._session is not None and not self._session.closed:
            try:
                await self._session.close()
            except Exception:
                pass


def chunk_list(xs: List, n: int) -> Iterator[List]:
    if n <= 0:
        yield xs
        return
    for i in range(0, len(xs), n):
        yield xs[i:i + n]

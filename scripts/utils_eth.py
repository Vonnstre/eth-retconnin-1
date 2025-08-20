# lightweight RPC pool (async) with retries and rotation across free public endpoints
import os
import asyncio
import aiohttp
import random
import json
from contextlib import asynccontextmanager
from typing import Iterator, List, Tuple, Union, Optional

FREE_RPC_FALLBACKS: List[str] = [
    "https://cloudflare-eth.com",
    "https://ethereum-rpc.publicnode.com",
    "https://rpc.ankr.com/eth",
    "https://eth.public-rpc.com",
    "https://1rpc.io/eth",
]


def _parse_urls(urls: Optional[Union[str, List[str]]]) -> List[str]:
    if isinstance(urls, str):
        return [u.strip() for u in urls.split(",") if u.strip()]
    return [u.strip() for u in (urls or []) if u.strip()]


class RpcPool:
    """
    Lightweight rotating RPC pool with retries + jitter.
    post(payload) accepts dict (single) or list (batch). Returns (ok, data).
    """
    def __init__(
        self,
        urls: Optional[Union[str, List[str]]] = None,
        concurrency: int = 12,
        timeout: int = 30,
        max_retries: int = 6,
    ):
        env_urls = _parse_urls(os.getenv("ETH_RPC_URL", ""))
        self.urls: List[str] = _parse_urls(urls) or env_urls or FREE_RPC_FALLBACKS[:]
        if not self.urls:
            raise ValueError("No RPC endpoints configured")
        self._rr = random.randint(0, len(self.urls) - 1)
        self._session: Optional[aiohttp.ClientSession] = None
        self.sem = asyncio.Semaphore(max(1, int(concurrency)))
        self.timeout = int(timeout)
        self.max_retries = int(max_retries)

    def _pick(self) -> str:
        url = self.urls[self._rr % len(self.urls)]
        self._rr += 1
        return url

    @asynccontextmanager
    async def _client(self):
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                raise_for_status=False,
                timeout=aiohttp.ClientTimeout(total=None),
                connector=aiohttp.TCPConnector(ssl=False, ttl_dns_cache=300),
                headers={"Content-Type": "application/json"},
            )
        try:
            yield self._session
        finally:
            # keep-alive; call close() explicitly when done
            pass

    async def post(self, payload, timeout: Optional[int] = None) -> Tuple[bool, object]:
        timeout = timeout or self.timeout
        async with self.sem:
            last_err = None
            for attempt in range(1, self.max_retries + 1):
                url = self._pick()
                try:
                    async with self._client() as client:
                        async with client.post(url, json=payload, timeout=timeout) as resp:
                            text = await resp.text()
                            if resp.status == 429:
                                last_err = f"429 {url}: {text[:160]}"
                                await asyncio.sleep(min(30, 0.6 * (2 ** attempt)) + random.random())
                                continue
                            if resp.status >= 500:
                                last_err = f"{resp.status} {url}: {text[:160]}"
                                await asyncio.sleep(min(30, 0.5 * (2 ** attempt)) + random.random())
                                continue
                            if resp.status != 200:
                                last_err = f"{resp.status} {url}: {text[:160]}"
                                await asyncio.sleep(min(8, 0.4 * attempt) + random.random() * 0.5)
                                continue
                            try:
                                data = json.loads(text)
                            except Exception:
                                return False, {"error": "invalid_json", "text": text[:400]}
                            if isinstance(data, dict) and "error" in data:
                                last_err = f"rpc_error {url}: {data['error']}"
                                await asyncio.sleep(min(8, 0.4 * attempt))
                                continue
                            return True, data
                except Exception as e:
                    last_err = f"{url}: {e}"
                    await asyncio.sleep(min(8, 0.5 * attempt) + random.random() * 0.5)
            return False, {"error": last_err}

    async def close(self):
        if self._session and not self._session.closed:
            try:
                await self._session.close()
            except Exception:
                pass


def chunk_list(xs: List, n: int):
    if n <= 0 or n >= len(xs):
        yield xs
        return
    for i in range(0, len(xs), n):
        yield xs[i : i + n]

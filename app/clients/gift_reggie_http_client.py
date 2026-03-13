from typing import Any, Dict, List, Optional
from datetime import datetime

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import json

from app.config.gift_reggie_config import AppConfig


class WishlistApiClient:
    def __init__(self, config: AppConfig) -> None:
        self._base_url = f"{config.api_base_url}/api/{config.store_id}"
        self._timeout = config.http_timeout_seconds

        self._token = config.api_token

        self._headers = self._build_headers()
        self._session = self._build_session()

    @staticmethod
    def _build_session() -> requests.Session:
        session = requests.Session()

        retry = Retry(
            total=5,
            backoff_factor=0.5,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("GET",),
            raise_on_status=False,
        )

        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    def _build_headers(self) -> Dict[str, str]:
        headers = {"Accept": "application/json"}

        # Only attach Authorization header if we're in header mode
        if self._token:
            headers["X-Access-Token"] = self._token
        return headers

    def _get_wishlists_page(
        self,
        *,
        page: int,
        rows: int,
        email: Optional[str] = None,
        customer_id: Optional[int] = None,
        updated: Optional[datetime] = None,
    ) -> List[Dict[str, Any]]:
        url = f"{self._base_url}/wishlists"

        params: Dict[str, Any] = {
            "page": page,
            "rows": rows
        }

        if email:
            params["email"] = email

        if customer_id is not None:
            params["customer_id"] = customer_id

        if updated is not None:
            params["updated"] = updated.strftime("%Y-%m-%dT%H:%M:%S")

        response = self._session.get(
            url,
            headers=self._headers, 
            params=params, 
            timeout=self._timeout,
        )

        if response.status_code >= 400:
            raise RuntimeError(f"Wishlist API error {response.status_code}: {response.text[:500]}")
        
        try:
            data = response.json()
        except ValueError as exc:
            raise RuntimeError(
                f"Wishlist API returned non-JSON response with status {response.status_code}: "
                f"{response.text[:500]}"
            ) from exc
        

        #print(json.dumps(data, indent=2, ensure_ascii=False, default=str))
        #print("\nIncpmiong Response Complete\n")

        if isinstance(data, list):
            return data

        if isinstance(data, dict) and isinstance(data.get("wishlists"), list):
            return data["wishlists"]

        raise RuntimeError(f"Unexpected response shape from /wishlists: {type(data)}")



    def get_all_wishlists(
        self,
        *,
        rows: int,
        email: Optional[str] = None,
        customer_id: Optional[int] = None,
        updated: Optional[datetime] = None,
        max_pages: int = 5,
    ) -> List[Dict[str, Any]]:
        page = 1
        all_items: List[Dict[str, Any]] = []

        while page <= max_pages:
            batch = self._get_wishlists_page(
                page=page,
                rows=rows,
                email=email,
                customer_id=customer_id,
                updated=updated,
            )

            if not batch:
                break

            all_items.extend(batch)

            if len(batch) < rows:
                break

            page += 1

        return all_items
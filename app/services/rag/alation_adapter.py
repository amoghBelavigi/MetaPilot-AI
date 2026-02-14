"""
Alation API Adapter

Thin abstraction layer for Alation REST APIs.
Handles authentication, error handling, and response mapping.
Provides clean interface for MCP server without exposing API complexity.

Key principles:
- Read-only operations
- Explicit handling of missing data (no hallucination)
- Graceful degradation on API failures
- Simple in-memory caching for performance
"""

import re
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
from dataclasses import dataclass
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


@dataclass
class CacheEntry:
    """Cache entry with TTL."""
    data: Any
    expires_at: datetime


class AlationAPIAdapter:
    """
    Adapter for Alation REST API.

    Provides clean, typed methods for accessing Alation metadata.
    All responses are read-only. Missing data is explicitly marked as "unknown".
    """

    # Cache TTL in seconds
    CACHE_TTL = 300  # 5 minutes

    def __init__(
        self,
        base_url: str,
        api_token: str,
        user_id: Optional[str] = None,
        cache_enabled: bool = True
    ):
        """
        Initialize Alation API adapter.

        Args:
            base_url: Alation instance URL (e.g., https://company.alation.com)
            api_token: Alation API token (can be a Refresh Token or API Access Token)
            user_id: Optional user ID for user-context operations
            cache_enabled: Enable in-memory caching
        """
        self.base_url = base_url.rstrip('/')
        self.api_token = api_token
        self.user_id = user_id
        self.cache_enabled = cache_enabled
        self._cache: Dict[str, CacheEntry] = {}

        # OPTIMIZED: Dedicated cache for table ID lookups (used by column & lineage queries)
        self._table_id_cache: Dict[str, int] = {}

        # Configure session with retry logic
        self.session = self._create_session()

        # Track whether auth has been validated against a live endpoint
        self._auth_validated = False

        # Try to authenticate -- if the token is a Refresh Token, exchange
        # it for an API Access Token automatically.
        self._ensure_valid_token()

    def _create_session(self) -> requests.Session:
        """Create requests session with retry logic. NO auth headers yet."""
        session = requests.Session()

        # Retry strategy for transient failures
        # Keep retries low to avoid long hangs that block Slack responses
        retry_strategy = Retry(
            total=1,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"]
        )

        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        # Only set Accept header -- auth headers are set after validation
        session.headers.update({
            "Accept": "application/json",
        })

        return session

    def _ensure_valid_token(self) -> None:
        """Find a working authentication method and configure the session.

        Strategy:
        1. Try the token directly with each header format (one at a time).
        2. If all direct attempts fail, try exchanging it as a Refresh Token.
        3. Try the exchanged token with each header format.
        4. Set the working header on the session for all future requests.
        """
        test_url = f"{self.base_url}/integration/v1/datasource/"

        # ── Step 1: Try the current token with different header formats ──
        # "TOKEN" is the standard Alation header; try it first
        auth_formats = [
            ("TOKEN", self.api_token),
            ("api-access-token", self.api_token),
        ]

        for header_name, token_value in auth_formats:
            try:
                logger.info(f"Testing auth header '{header_name}' ...")
                resp = requests.get(
                    test_url,
                    headers={"Accept": "application/json", header_name: token_value},
                    timeout=10,
                )
                logger.info(f"  → {header_name}: HTTP {resp.status_code} | {resp.text[:120]}")
                if resp.status_code != 403:
                    # This header format works!
                    self.session.headers[header_name] = token_value
                    self._auth_validated = True
                    logger.info(f"Auth OK using header '{header_name}'")
                    return
            except requests.exceptions.RequestException as e:
                logger.warning(f"  → {header_name}: request failed ({e})")
                # Network error -- set token anyway so it works when network returns,
                # then skip to refresh token exchange attempt
                self.session.headers["TOKEN"] = token_value
                logger.info(
                    "Network unreachable (VPN?). Set TOKEN header anyway "
                    "so requests work when network returns."
                )
                self._auth_validated = False
                return

        # ── Step 2: Direct auth failed → try Refresh Token exchange ──
        logger.info(
            "Direct auth failed with all header formats. "
            "Attempting Refresh Token → API Access Token exchange..."
        )
        access_token = self._exchange_refresh_token()

        if access_token:
            self.api_token = access_token
            # Try the new token with each header format
            for header_name in ["TOKEN", "api-access-token"]:
                try:
                    resp = requests.get(
                        test_url,
                        headers={"Accept": "application/json", header_name: access_token},
                        timeout=10,
                    )
                    logger.info(
                        f"Exchanged token with '{header_name}': "
                        f"HTTP {resp.status_code}"
                    )
                    if resp.status_code != 403:
                        self.session.headers[header_name] = access_token
                        self._auth_validated = True
                        logger.info(f"Auth OK after exchange using '{header_name}'")
                        return
                except requests.exceptions.RequestException:
                    pass

            # Exchange succeeded but the new token also gets 403 --
            # set it anyway and hope specific endpoints differ
            self.session.headers["TOKEN"] = access_token
            logger.warning(
                "Exchanged token also got 403 on test endpoint. "
                "Setting header anyway -- specific endpoints may work."
            )
        else:
            # Exchange failed -- set original token with TOKEN header
            self.session.headers["TOKEN"] = self.api_token
            logger.error(
                "All auth methods failed. Set the token anyway. "
                "Requests will likely fail with 403."
            )

    def _exchange_refresh_token(self) -> Optional[str]:
        """Exchange a Refresh Token for an API Access Token.

        IMPORTANT: Uses raw requests.post() instead of self.session to avoid
        sending conflicting auth headers that may confuse the exchange endpoint.

        Returns:
            API Access Token string, or None on failure
        """
        refresh_token = self.api_token

        if not self.user_id:
            logger.error(
                "ALATION_USER_ID is required for Refresh Token exchange "
                "but is not set. Please add it to your .env file. "
                "Find your user ID in Alation: log in → click your profile → "
                "the URL shows /user/<ID>/"
            )
            return None

        # Method 1: POST /integration/v1/createAPIAccessToken/
        try:
            url = f"{self.base_url}/integration/v1/createAPIAccessToken/"
            payload = {
                "refresh_token": refresh_token,
                "user_id": int(self.user_id),
            }

            logger.info(f"Token exchange v1: POST {url}")
            # Use raw requests -- NOT self.session (no conflicting headers)
            resp = requests.post(
                url,
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=15,
            )
            logger.info(
                f"  → v1 response: {resp.status_code} | {resp.text[:200]}"
            )

            # Alation returns 201 Created (not 200) for token creation
            if resp.status_code in (200, 201):
                data = resp.json()
                token = (
                    data.get("api_access_token")
                    or data.get("token")
                    or data.get("access_token")
                )
                if token:
                    logger.info("Refresh Token exchanged successfully (v1)")
                    return token

        except Exception as e:
            logger.error(f"Token exchange v1 failed: {e}")

        # Method 2: POST /integration/v2/createAPIAccessToken/
        try:
            url = f"{self.base_url}/integration/v2/createAPIAccessToken/"
            headers = {
                "Authorization": f"Bearer {refresh_token}",
                "Content-Type": "application/json",
            }
            payload = {"user_id": int(self.user_id)} if self.user_id else {}

            logger.info(f"Token exchange v2: POST {url}")
            resp = requests.post(
                url,
                json=payload,
                headers=headers,
                timeout=15,
            )
            logger.info(
                f"  → v2 response: {resp.status_code} | {resp.text[:200]}"
            )

            # Accept both 200 and 201
            if resp.status_code in (200, 201):
                data = resp.json()
                token = (
                    data.get("api_access_token")
                    or data.get("token")
                    or data.get("access_token")
                )
                if token:
                    logger.info("Refresh Token exchanged successfully (v2)")
                    return token

        except Exception as e:
            logger.error(f"Token exchange v2 failed: {e}")

        logger.error(
            "Token exchange failed with both v1 and v2 endpoints. "
            "The ALATION_API_TOKEN may be invalid or expired."
        )
        return None

    def _get_from_cache(self, key: str) -> Optional[Any]:
        """Retrieve item from cache if valid."""
        if not self.cache_enabled:
            return None

        entry = self._cache.get(key)
        if entry and datetime.now() < entry.expires_at:
            logger.debug(f"Cache hit for key: {key}")
            return entry.data

        # Remove expired entry
        if entry:
            del self._cache[key]

        return None

    def _set_in_cache(self, key: str, data: Any) -> None:
        """Store item in cache with TTL."""
        if not self.cache_enabled:
            return

        expires_at = datetime.now() + timedelta(seconds=self.CACHE_TTL)
        self._cache[key] = CacheEntry(data=data, expires_at=expires_at)
        logger.debug(f"Cache set for key: {key}")

    def _api_request(
        self,
        endpoint: str,
        params: Optional[Dict] = None,
        cache_key: Optional[str] = None
    ) -> Optional[Dict]:
        """
        Make API request to Alation.

        Args:
            endpoint: API endpoint path (e.g., '/integration/v1/datasource/')
            params: Query parameters
            cache_key: Optional cache key for result

        Returns:
            API response data or None on failure
        """
        # Check cache first
        if cache_key:
            cached = self._get_from_cache(cache_key)
            if cached is not None:
                return cached

        url = f"{self.base_url}{endpoint}"

        try:
            logger.info(f"Alation API request: {endpoint} params={params}")
            response = self.session.get(url, params=params, timeout=15)

            # Log ALL responses (not just errors) for diagnosis
            body_preview = response.text[:200] if response.text else "(empty)"
            logger.info(
                f"  → {endpoint}: HTTP {response.status_code} | "
                f"len={len(response.text)} | {body_preview}"
            )

            response.raise_for_status()

            data = response.json()

            # Cache successful response
            if cache_key:
                self._set_in_cache(cache_key, data)

            return data

        except requests.exceptions.HTTPError as e:
            status = e.response.status_code
            # Log response body for diagnosis (truncated)
            try:
                body = e.response.text[:300]
            except Exception:
                body = "(unable to read response body)"
            if status == 404:
                logger.warning(f"Resource not found: {endpoint}")
            elif status == 403:
                logger.error(
                    f"403 Access denied: {endpoint} | "
                    f"Response: {body}"
                )
                # If auth was never validated (e.g. network was down at startup),
                # try re-authenticating now and retry the request ONCE.
                if not self._auth_validated:
                    logger.info("Auth not yet validated -- attempting re-authentication...")
                    self._ensure_valid_token()
                    if self._auth_validated:
                        logger.info("Re-auth succeeded, retrying request...")
                        try:
                            response = self.session.get(url, params=params, timeout=15)
                            response.raise_for_status()
                            data = response.json()
                            if cache_key:
                                self._set_in_cache(cache_key, data)
                            return data
                        except Exception as retry_err:
                            logger.error(f"Retry after re-auth also failed: {retry_err}")
                            return None
            else:
                logger.error(
                    f"HTTP {status}: {endpoint} | "
                    f"Response: {body}"
                )
            return None

        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed for {endpoint}: {str(e)}")
            return None

        except Exception as e:
            logger.error(f"Unexpected error for {endpoint}: {str(e)}")
            return None

    # =========================================================================
    # Data Source Operations
    # =========================================================================

    def list_data_sources(self) -> List[Dict[str, Any]]:
        """
        List all accessible data sources.

        Tries both v1 and v2 endpoints and merges results, since
        the token may have different visibility in each API version.

        Returns:
            List of data sources with id, name, type, description
        """
        seen_ids = set()
        data_sources = []

        # Try v1 first (older, sometimes has broader access)
        data = self._api_request(
            '/integration/v1/datasource/',
            cache_key='data_sources_v1'
        )
        if data:
            for ds in data:
                ds_id = ds.get('id')
                if ds_id not in seen_ids:
                    seen_ids.add(ds_id)
                    data_sources.append({
                        'data_source_id': ds_id,
                        'name': ds.get('title', 'unknown'),
                        'type': ds.get('dbtype', 'unknown'),
                        'description': self._strip_html(
                            ds.get('description', 'unknown')
                        ),
                    })

        # Also try v2 (may reveal data sources not visible via v1)
        data_v2 = self._api_request(
            '/integration/v2/datasource/',
            cache_key='data_sources_v2'
        )
        if data_v2:
            for ds in data_v2:
                ds_id = ds.get('id')
                if ds_id not in seen_ids:
                    seen_ids.add(ds_id)
                    data_sources.append({
                        'data_source_id': ds_id,
                        'name': ds.get('title', ds.get('name', 'unknown')),
                        'type': ds.get('dbtype', ds.get('db_type', 'unknown')),
                        'description': self._strip_html(
                            ds.get('description', 'unknown')
                        ),
                    })

        if not data_sources:
            logger.warning("Failed to retrieve data sources from Alation (v1 + v2)")

        logger.info(f"Listed {len(data_sources)} data sources")
        return data_sources

    def get_data_source(self, data_source_id: int) -> Optional[Dict[str, Any]]:
        """
        Get details for a specific data source.

        Args:
            data_source_id: Alation data source ID

        Returns:
            Data source details or None
        """
        data = self._api_request(
            f'/integration/v1/datasource/{data_source_id}/',
            cache_key=f'ds_{data_source_id}'
        )

        if not data:
            return None

        return {
            'data_source_id': data.get('id'),
            'name': data.get('title', 'unknown'),
            'type': data.get('dbtype', 'unknown'),
            'description': data.get('description', 'unknown'),
            'uri': data.get('uri', 'unknown')
        }

    # =========================================================================
    # Schema Operations
    # =========================================================================

    def list_schemas(self, data_source_id: int) -> List[Dict[str, Any]]:
        """
        List schemas in a data source.

        Args:
            data_source_id: Alation data source ID

        Returns:
            List of schemas with name and description
        """
        # Alation API endpoint for schemas
        data = self._api_request(
            '/integration/v2/schema/',
            params={'ds_id': data_source_id},
            cache_key=f'schemas_{data_source_id}'
        )

        if not data:
            logger.warning(f"No schemas found for data source {data_source_id}")
            return []

        schemas = []
        for schema in data:
            schemas.append({
                'schema_name': schema.get('name', 'unknown'),
                'schema_description': self._strip_html(
                    schema.get('description', 'unknown')
                )
            })

        return schemas

    # =========================================================================
    # Table Operations
    # =========================================================================

    def list_tables(
        self,
        data_source_id: int,
        schema_name: str
    ) -> List[Dict[str, Any]]:
        """
        List tables in a schema.

        Args:
            data_source_id: Alation data source ID
            schema_name: Schema name

        Returns:
            List of tables with metadata
        """
        data = self._api_request(
            '/integration/v2/table/',
            params={
                'ds_id': data_source_id,
                'schema_name': schema_name
            },
            cache_key=f'tables_{data_source_id}_{schema_name}'
        )

        if not data:
            logger.warning(f"No tables found for {data_source_id}.{schema_name}")
            return []

        tables = []
        for table in data:
            tables.append({
                'table_name': table.get('name', 'unknown'),
                'table_type': table.get('table_type', 'unknown'),
                'row_count': table.get('number_of_rows', 'unknown'),
                'popularity': table.get('popularity', 'unknown')
            })

        return tables

    def get_table_metadata(
        self,
        data_source_id: int,
        schema_name: str,
        table_name: str
    ) -> Optional[Dict[str, Any]]:
        """
        Get detailed metadata for a specific table.

        Args:
            data_source_id: Alation data source ID
            schema_name: Schema name
            table_name: Table name

        Returns:
            Table metadata including description, owner, certification, etc.
        """
        # Get table by qualified name
        data = self._api_request(
            '/integration/v2/table/',
            params={
                'ds_id': data_source_id,
                'schema_name': schema_name,
                'name': table_name
            },
            cache_key=f'table_meta_{data_source_id}_{schema_name}_{table_name}'
        )

        if not data or len(data) == 0:
            logger.warning(f"Table not found: {data_source_id}.{schema_name}.{table_name}")
            return None

        # Alation returns list, take first match
        table = data[0]

        return {
            'table_name': table.get('name', 'unknown'),
            'table_description': self._strip_html(table.get('description', 'unknown')),
            'owner': table.get('owner', 'unknown'),
            'steward': table.get('steward', 'unknown'),
            'certification': table.get('trust_flags', {}).get('certification', 'unknown'),
            'trust_status': table.get('trust_flags', {}).get('endorsement', 'unknown'),
            'last_updated': table.get('ts_updated', 'unknown'),
        }

    # =========================================================================
    # Table ID Lookup (OPTIMIZED with dedicated cache)
    # =========================================================================

    def _get_table_id(
        self,
        data_source_id: int,
        schema_name: str,
        table_name: str
    ) -> Optional[int]:
        """
        Get table ID with dedicated caching.

        Tries multiple API endpoints for compatibility across Alation
        versions and permission levels.

        Args:
            data_source_id: Alation data source ID
            schema_name: Schema name
            table_name: Table name

        Returns:
            Table ID or None if not found
        """
        cache_key = f"{data_source_id}_{schema_name}_{table_name}"

        # Check dedicated table ID cache first
        if cache_key in self._table_id_cache:
            logger.debug(f"Table ID cache hit: {cache_key}")
            return self._table_id_cache[cache_key]

        # Try multiple endpoints and filter combinations to find the table
        table_id = None

        # Attempt 1: Integration API v2 with schema + name
        logger.info(f"[Table ID] Attempt 1: /integration/v2/table/ with schema={schema_name}, name={table_name}")
        table_data = self._api_request(
            '/integration/v2/table/',
            params={
                'ds_id': data_source_id,
                'schema_name': schema_name,
                'name': table_name
            }
        )
        if table_data and len(table_data) > 0:
            table_id = table_data[0].get('id')

        # Attempt 2: Integration API v2 with name ONLY (no schema filter)
        # Schema name format might not match Alation's internal representation
        if not table_id:
            logger.info(f"[Table ID] Attempt 2: /integration/v2/table/ with name only")
            table_data = self._api_request(
                '/integration/v2/table/',
                params={
                    'ds_id': data_source_id,
                    'name': table_name
                }
            )
            if table_data and len(table_data) > 0:
                table_id = table_data[0].get('id')
                # Log what schema Alation actually has
                actual_schema = table_data[0].get('schema_name', table_data[0].get('schema', 'unknown'))
                logger.info(f"[Table ID] Found table! Alation schema = '{actual_schema}'")

        # Attempt 3: CROSS-DATA-SOURCE search (no ds_id filter)
        # The token may not have permission to list the data source via v1,
        # but CAN access its tables via v2. Search by name across ALL sources.
        if not table_id:
            logger.info(f"[Table ID] Attempt 3: Cross-data-source search by name={table_name}")
            table_data = self._api_request(
                '/integration/v2/table/',
                params={'name': table_name}
            )
            if table_data and len(table_data) > 0:
                # Prefer exact match
                for t in table_data:
                    if t.get('name', '').upper() == table_name.upper():
                        table_id = t.get('id')
                        actual_ds = t.get('ds_id')
                        actual_schema = t.get('schema_name', 'unknown')
                        logger.info(
                            f"[Table ID] Found via cross-ds search! "
                            f"table_id={table_id}, ds_id={actual_ds}, "
                            f"schema='{actual_schema}'"
                        )
                        break
                if not table_id:
                    table_id = table_data[0].get('id')
                    logger.info(f"[Table ID] Using first match: table_id={table_id}")

        if table_id:
            self._table_id_cache[cache_key] = table_id
            logger.info(f"Found table_id={table_id} for {schema_name}.{table_name}")
        else:
            logger.warning(f"Table not found via any API: {schema_name}.{table_name}")

        return table_id

    # =========================================================================
    # Column Operations
    # =========================================================================

    def get_column_metadata(
        self,
        data_source_id: int,
        schema_name: str,
        table_name: str
    ) -> List[Dict[str, Any]]:
        """
        Get column metadata for a table.

        Tries multiple API families and endpoints to maximize compatibility
        across Alation versions and token permission levels.

        Args:
            data_source_id: Alation data source ID
            schema_name: Schema name
            table_name: Table name

        Returns:
            List of column metadata including types, descriptions, classifications
        """
        qualified_table_name = f"{schema_name}.{table_name}"
        cache_key = f'columns_{data_source_id}_{schema_name}_{table_name}'

        # Check cache first
        cached = self._get_from_cache(cache_key)
        if cached is not None:
            return cached

        # ==================================================================
        # STRATEGY: First find the table_id, then get columns by table_id.
        # This is the most reliable approach since /integration/v2/column/
        # requires table_id, not table_name.
        # ==================================================================

        # -----------------------------------------------------------------
        # Step 1: Find the table ID
        # -----------------------------------------------------------------
        table_id = self._get_table_id(data_source_id, schema_name, table_name)

        if table_id:
            # -----------------------------------------------------------------
            # Approach 1: Integration API v2 /column/ with table_id
            # -----------------------------------------------------------------
            logger.info(f"[Columns] Approach 1: /integration/v2/column/ with table_id={table_id}")
            data = self._api_request(
                '/integration/v2/column/',
                params={'table_id': table_id}
            )
            if data and len(data) > 0:
                logger.info(f"Approach 1 succeeded: {len(data)} columns")
                result = self._parse_columns(data)
                self._set_in_cache(cache_key, result)
                return result

            # -----------------------------------------------------------------
            # Approach 2: Legacy attribute API with table_id
            # -----------------------------------------------------------------
            logger.info(f"[Columns] Approach 2: /api/v1/attribute/ with table_id={table_id}")
            data = self._api_request(
                '/api/v1/attribute/',
                params={'table_id': table_id}
            )
            if data and len(data) > 0:
                logger.info(f"Approach 2 succeeded: {len(data)} columns")
                result = self._parse_columns(data)
                self._set_in_cache(cache_key, result)
                return result

            # -----------------------------------------------------------------
            # Approach 3: Catalog table detail with embedded columns
            # -----------------------------------------------------------------
            logger.info(f"[Columns] Approach 3: /catalog/table/{table_id}/ detail")
            table_detail = self._api_request(f'/catalog/table/{table_id}/')
            if table_detail:
                cols = table_detail.get('columns', [])
                if cols:
                    logger.info(f"Approach 3 succeeded: {len(cols)} columns")
                    result = self._parse_columns(cols)
                    self._set_in_cache(cache_key, result)
                    return result

        # -----------------------------------------------------------------
        # Fallback: Try column endpoint with name-based filters
        # (in case table_id lookup failed)
        # -----------------------------------------------------------------
        logger.info(f"[Columns] Fallback: /integration/v2/column/ with table_name filters")
        for tname in [qualified_table_name, table_name]:
            data = self._api_request(
                '/integration/v2/column/',
                params={'ds_id': data_source_id, 'table_name': tname}
            )
            if data and len(data) > 0:
                logger.info(f"Fallback succeeded with table_name='{tname}': {len(data)} columns")
                result = self._parse_columns(data)
                self._set_in_cache(cache_key, result)
                return result

        logger.warning(
            f"No columns found for {qualified_table_name} "
            f"after trying all API approaches"
        )
        return []

    @staticmethod
    def _strip_html(text: str) -> str:
        """Strip HTML tags and clean up whitespace from a string."""
        if not text or not isinstance(text, str):
            return str(text) if text else "unknown"
        # Remove HTML tags
        clean = re.sub(r'<[^>]+>', '', text)
        # Collapse whitespace
        clean = re.sub(r'\s+', ' ', clean).strip()
        # Truncate long descriptions to keep LLM context manageable
        if len(clean) > 200:
            clean = clean[:200] + "..."
        return clean or "unknown"

    def _parse_columns(self, data: List[Dict]) -> List[Dict[str, Any]]:
        """Parse column data from any Alation API response.

        Handles field name variations across Integration, Catalog, and
        legacy API responses. Strips HTML and limits data size to keep
        the LLM context within token limits.

        Args:
            data: Raw column data from Alation API

        Returns:
            Normalized list of column metadata dicts (compact format)
        """
        columns = []
        for col in data:
            # Get description, strip HTML
            raw_desc = col.get('description', 'unknown')
            desc = self._strip_html(raw_desc)

            # Get title (often a clean human-readable name)
            title = col.get('title', '')

            columns.append({
                'column_name': col.get('name', col.get('title', 'unknown')),
                'data_type': col.get('column_type',
                             col.get('data_type',
                             col.get('type', 'unknown'))),
                'description': desc,
                'title': title if title else 'unknown',
                'nullable': col.get('nullable', 'unknown'),
            })
        return columns

    # =========================================================================
    # Lineage Operations
    # =========================================================================

    def get_lineage(
        self,
        data_source_id: int,
        schema_name: str,
        table_name: str
    ) -> Dict[str, Any]:
        """
        Get lineage for a table.

        Args:
            data_source_id: Alation data source ID
            schema_name: Schema name
            table_name: Table name

        Returns:
            Lineage information with upstream and downstream tables
        """
        # Get table ID using cached lookup (OPTIMIZED)
        table_id = self._get_table_id(data_source_id, schema_name, table_name)

        if not table_id:
            logger.warning(f"Cannot get lineage - table not found: {table_name}")
            return {
                'upstream_tables': 'unknown',
                'downstream_tables': 'unknown',
                'transformation_context': 'unknown'
            }

        # Get lineage using lineage API
        lineage_data = self._api_request(
            f'/integration/v2/lineage/',
            params={'oid': table_id, 'otype': 'table'},
            cache_key=f'lineage_{data_source_id}_{schema_name}_{table_name}'
        )

        if not lineage_data:
            logger.warning(f"Lineage data not available for {table_name}")
            return {
                'upstream_tables': 'unknown',
                'downstream_tables': 'unknown',
                'transformation_context': 'unknown'
            }

        # Parse lineage response
        upstream = lineage_data.get('upstream', [])
        downstream = lineage_data.get('downstream', [])

        return {
            'upstream_tables': [u.get('key', 'unknown') for u in upstream] if upstream else 'unknown',
            'downstream_tables': [d.get('key', 'unknown') for d in downstream] if downstream else 'unknown',
            'transformation_context': lineage_data.get('sql', 'unknown')
        }

    # =========================================================================
    # Search Operations (cross-data-source)
    # =========================================================================

    def search_table(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Search for a table by name across ALL data sources.

        This is the fastest way to find a table when you know its name
        but not which data source or schema it belongs to.

        Strategy:
        1. Try direct name search (no ds_id) -- works on some Alation versions
        2. If empty, iterate over all known data sources and search each one
        3. Also try Alation search API as fallback

        Args:
            table_name: Table name to search for (case-insensitive)

        Returns:
            List of matching tables with data_source_id, schema, and metadata
        """
        logger.info(f"Searching for table '{table_name}' across all data sources")

        results = []
        seen_ids = set()

        def _add_results(data):
            """Helper to parse table results and avoid duplicates."""
            if not data:
                return
            for t in data:
                tid = t.get('id')
                if tid and tid in seen_ids:
                    continue
                if tid:
                    seen_ids.add(tid)
                # Extract schema from key (format: "ds_id.SCHEMA.TABLE")
                key = t.get('key', '')
                schema = 'unknown'
                if key:
                    parts = key.split('.')
                    if len(parts) >= 3:
                        # key = "83.PS_PRD_01_USERFP.PUBLIC.TABLE_NAME"
                        schema = '.'.join(parts[1:-1])
                results.append({
                    'table_name': t.get('name', 'unknown'),
                    'data_source_id': t.get('ds_id', 'unknown'),
                    'schema_name': t.get('schema_name', schema),
                    'table_id': tid,
                    'table_type': t.get('table_type', 'unknown'),
                    'description': self._strip_html(t.get('description', 'unknown')),
                    'url': t.get('url', 'unknown'),
                })

        # --- Attempt 1: Direct name search (no ds_id) ---
        # Try both exact name and case variants
        for name_variant in [table_name, table_name.upper(), table_name.lower()]:
            data = self._api_request(
                '/integration/v2/table/',
                params={'name': name_variant}
            )
            _add_results(data)
            if results:
                break

        # --- Attempt 2: Iterate over all data sources with exact name ---
        if not results:
            logger.info(
                f"Direct search empty. Searching each data source for '{table_name}'..."
            )
            data_sources = self.list_data_sources()
            for ds in data_sources:
                ds_id = ds.get('data_source_id')
                if ds_id is None:
                    continue
                # Try exact name
                data = self._api_request(
                    '/integration/v2/table/',
                    params={'ds_id': ds_id, 'name': table_name}
                )
                _add_results(data)

        # --- Attempt 3: Alation search API (with correct params) ---
        if not results:
            logger.info(f"Table not found via v2. Trying search API...")
            # Try multiple search endpoints and param combos
            search_combos = [
                ('/integration/v1/search/', {'q': table_name, 'otype': 'table', 'limit': 10}),
                ('/integration/v1/search/', {'q': table_name, 'limit': 10}),
                ('/search/', {'q': table_name, 'otype': 'table', 'limit': 10}),
            ]
            for endpoint, params in search_combos:
                data = self._api_request(endpoint, params=params)
                if data and isinstance(data, list):
                    for item in data:
                        obj = item if isinstance(item, dict) else {}
                        tid = obj.get('id')
                        if tid and tid not in seen_ids:
                            seen_ids.add(tid)
                            results.append({
                                'table_name': obj.get('name', obj.get('title', 'unknown')),
                                'data_source_id': obj.get('ds_id', 'unknown'),
                                'schema_name': obj.get('schema_name', 'unknown'),
                                'table_id': tid,
                                'table_type': obj.get('table_type', 'unknown'),
                                'description': self._strip_html(
                                    obj.get('description', 'unknown')
                                ),
                                'url': obj.get('url', 'unknown'),
                            })
                    if results:
                        break

        # --- Attempt 4: Keyword search in table names within each schema ---
        # The v2 API name filter may do exact match only. Try getting
        # all tables from likely schemas and filter locally.
        if not results:
            logger.info(
                f"Exact name match failed everywhere. "
                f"Trying keyword-in-key search..."
            )
            # Use the catalog search: /integration/v2/table/ supports
            # searching by key which contains the full qualified name
            data = self._api_request(
                '/integration/v2/table/',
                params={'search': table_name}
            )
            _add_results(data)

        logger.info(f"Table search for '{table_name}': {len(results)} result(s)")
        return results

    def search_schema(self, keyword: str) -> List[Dict[str, Any]]:
        """
        Search for schemas matching a keyword across ALL data sources.

        Useful when you know a schema/database name but not which
        Alation data source it belongs to.

        Args:
            keyword: Keyword to search for in schema names (case-insensitive)

        Returns:
            List of matching schemas with data_source_id and names
        """
        logger.info(f"Searching for schemas matching '{keyword}'")

        results = []

        # Get all data sources, then search their schemas
        # First check if we have data sources cached
        data_sources = self.list_data_sources()

        for ds in data_sources:
            ds_id = ds.get('data_source_id')
            if ds_id is None:
                continue

            schemas = self._api_request(
                '/integration/v2/schema/',
                params={'ds_id': ds_id},
                cache_key=f'schemas_{ds_id}'
            )

            if not schemas:
                continue

            for schema in schemas:
                schema_name = schema.get('name', '')
                if keyword.lower() in schema_name.lower():
                    results.append({
                        'schema_name': schema_name,
                        'data_source_id': ds_id,
                        'data_source_name': ds.get('name', 'unknown'),
                        'description': self._strip_html(
                            schema.get('description', 'unknown')
                        ),
                    })

        logger.info(f"Schema search for '{keyword}': {len(results)} result(s)")
        return results

    def search_columns(
        self, column_name: str, table_name: str = None
    ) -> List[Dict[str, Any]]:
        """
        Search for columns by name, optionally filtered to a specific table.

        Useful when the user asks about a specific column (e.g. TXN_DTTM)
        and you need to find which tables contain it and what it means.

        Args:
            column_name: Column name to search for
            table_name: Optional table name to filter results

        Returns:
            List of matching columns with table context
        """
        logger.info(f"Searching for column '{column_name}' (table_filter={table_name})")

        results = []
        params = {'name': column_name}

        data = self._api_request('/integration/v2/column/', params=params)

        if data:
            for col in data:
                # If table filter specified, check the key
                if table_name:
                    col_key = col.get('key', '')
                    if table_name.lower() not in col_key.lower():
                        continue

                results.append({
                    'column_name': col.get('name', 'unknown'),
                    'data_type': col.get('column_type',
                                 col.get('data_type',
                                 col.get('type', 'unknown'))),
                    'description': self._strip_html(
                        col.get('description', 'unknown')
                    ),
                    'title': col.get('title', 'unknown'),
                    'table_key': col.get('key', 'unknown'),
                    'nullable': col.get('nullable', 'unknown'),
                })

        logger.info(f"Column search for '{column_name}': {len(results)} result(s)")
        return results[:30]  # Limit to 30 results

    def clear_cache(self) -> None:
        """Clear all cached data including table ID cache."""
        self._cache.clear()
        self._table_id_cache.clear()
        logger.info("All caches cleared")

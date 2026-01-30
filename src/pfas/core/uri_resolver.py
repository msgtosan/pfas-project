"""
URI Resolver for Multi-User File Isolation.

Provides portable URI-based file addressing with user namespace prefixing:
- Converts absolute paths to portable URIs
- Resolves URIs back to absolute paths
- Ensures database portability across environments
- Supports multi-tenant isolation at storage layer

URI Format:
    pfas://users/{user}/archive/{category}/{file}
    pfas://users/{user}/inbox/{category}/{file}
    pfas://users/{user}/config/{file}
    pfas://users/{user}/reports/{category}/{file}

Usage:
    resolver = PFASURIResolver(data_root=Path("/data/pfas"))

    # Convert path to URI
    uri = resolver.to_uri(Path("/data/pfas/Users/Sanjay/archive/Mutual-Fund/cas.pdf"))
    # Returns: "pfas://users/sanjay/archive/mutual-fund/cas.pdf"

    # Resolve URI to path
    path = resolver.resolve("pfas://users/sanjay/archive/mutual-fund/cas.pdf")
    # Returns: Path("/data/pfas/Users/Sanjay/archive/Mutual-Fund/cas.pdf")
"""

import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Tuple, Dict, Any
from urllib.parse import urlparse, quote, unquote

logger = logging.getLogger(__name__)


# URI scheme for PFAS files
PFAS_SCHEME = "pfas"


@dataclass
class ParsedURI:
    """Parsed components of a PFAS URI."""

    scheme: str  # Always "pfas"
    user_namespace: str  # User name (lowercase)
    area: str  # inbox, archive, config, reports, db
    category: Optional[str]  # Sub-category like "Mutual-Fund"
    filename: str  # Actual filename
    relative_path: str  # Full relative path from user directory

    @property
    def is_valid(self) -> bool:
        """Check if URI is valid."""
        return (
            self.scheme == PFAS_SCHEME
            and bool(self.user_namespace)
            and bool(self.area)
        )


class URIResolutionError(Exception):
    """Error resolving PFAS URI."""
    pass


class PFASURIResolver:
    """
    Resolves PFAS URIs to filesystem paths and vice versa.

    The URI format provides:
    - Database portability: URIs stored in DB work across environments
    - Multi-user isolation: Each user's files are namespaced
    - Path normalization: Consistent lowercase URIs, case-preserved paths

    URI Structure:
        pfas://users/{user}/{area}/{category}/{filename}

    Areas:
        - inbox: Incoming files awaiting processing
        - archive: Processed/archived files
        - config: User configuration files
        - reports: Generated reports
        - db: Database files
        - golden: Golden reference files
    """

    # Mapping from URI area to config key
    AREA_MAPPING = {
        "inbox": "inbox",
        "archive": "archive",
        "config": "user_config_dir",
        "reports": "reports",
        "db": "db_file",
        "golden": "golden",
    }

    def __init__(
        self,
        data_root: Path,
        users_base: str = "Users"
    ):
        """
        Initialize URI resolver.

        Args:
            data_root: Root data directory (e.g., /data/pfas or Data/)
            users_base: Subdirectory containing user directories (default: "Users")
        """
        self.data_root = Path(data_root).resolve()
        self.users_base = users_base

        # Case mapping for category names (URI lowercase -> filesystem case)
        self._category_case_map: Dict[str, str] = {}
        self._build_case_map()

    def _build_case_map(self) -> None:
        """Build case mapping from filesystem."""
        users_path = self.data_root / self.users_base

        if not users_path.exists():
            return

        for user_dir in users_path.iterdir():
            if not user_dir.is_dir():
                continue

            # Map user name
            self._category_case_map[user_dir.name.lower()] = user_dir.name

            # Map subdirectories
            for subdir in user_dir.iterdir():
                if subdir.is_dir():
                    key = f"{user_dir.name.lower()}/{subdir.name.lower()}"
                    self._category_case_map[key] = subdir.name

                    # Map category subdirectories
                    for category in subdir.iterdir():
                        if category.is_dir():
                            cat_key = f"{user_dir.name.lower()}/{subdir.name.lower()}/{category.name.lower()}"
                            self._category_case_map[cat_key] = category.name

    def _get_filesystem_case(self, user: str, *parts: str) -> str:
        """Get filesystem case for path components."""
        key = user.lower()
        result = self._category_case_map.get(key, user)

        for part in parts:
            key = f"{key}/{part.lower()}"
            part_case = self._category_case_map.get(key, part)
            result = part_case  # Return the last matched part

        return result

    def to_uri(self, absolute_path: Path) -> str:
        """
        Convert absolute filesystem path to PFAS URI.

        Args:
            absolute_path: Absolute path to file

        Returns:
            PFAS URI string

        Raises:
            URIResolutionError: If path cannot be converted
        """
        absolute_path = Path(absolute_path).resolve()

        # Check if path is under data root
        try:
            relative = absolute_path.relative_to(self.data_root)
        except ValueError:
            raise URIResolutionError(
                f"Path not under data root: {absolute_path}"
            )

        parts = relative.parts

        # Expected structure: Users/{user}/{area}/...
        if len(parts) < 3:
            raise URIResolutionError(
                f"Path too short to resolve: {absolute_path}"
            )

        if parts[0] != self.users_base:
            raise URIResolutionError(
                f"Path not in users base: {absolute_path}"
            )

        user = parts[1]
        area = parts[2].lower()

        # Build relative path from area
        if len(parts) > 3:
            file_parts = parts[3:]
            # Lowercase for URI normalization
            normalized_parts = [p.lower() for p in file_parts[:-1]]
            # Keep filename as-is for readability
            filename = file_parts[-1]
            relative_path = "/".join(normalized_parts + [filename])
        else:
            relative_path = ""

        # Build URI
        uri = f"{PFAS_SCHEME}://users/{user.lower()}/{area}"
        if relative_path:
            uri = f"{uri}/{relative_path}"

        return uri

    def resolve(
        self,
        uri: str,
        user_context: Optional[str] = None
    ) -> Path:
        """
        Resolve PFAS URI to absolute filesystem path.

        Args:
            uri: PFAS URI string
            user_context: Optional user context for relative URIs

        Returns:
            Absolute Path object

        Raises:
            URIResolutionError: If URI cannot be resolved
        """
        parsed = self.parse(uri)

        if not parsed.is_valid:
            raise URIResolutionError(f"Invalid URI: {uri}")

        # Get user directory with proper case
        user_dir_name = self._get_filesystem_case(parsed.user_namespace)
        user_path = self.data_root / self.users_base / user_dir_name

        # Get area directory with proper case
        area_name = self._get_filesystem_case(
            parsed.user_namespace,
            parsed.area
        )

        # Build full path
        if parsed.category:
            category_name = self._get_filesystem_case(
                parsed.user_namespace,
                parsed.area,
                parsed.category
            )
            full_path = user_path / area_name / category_name / parsed.filename
        elif parsed.relative_path:
            # Reconstruct path preserving case from filesystem
            full_path = user_path / area_name / parsed.relative_path
        else:
            full_path = user_path / area_name

        return full_path

    def parse(self, uri: str) -> ParsedURI:
        """
        Parse PFAS URI into components.

        Args:
            uri: PFAS URI string

        Returns:
            ParsedURI with components
        """
        # Handle URL encoding
        uri = unquote(uri)

        # Parse URI
        if not uri.startswith(f"{PFAS_SCHEME}://"):
            return ParsedURI(
                scheme="",
                user_namespace="",
                area="",
                category=None,
                filename="",
                relative_path="",
            )

        # Remove scheme
        path_part = uri[len(f"{PFAS_SCHEME}://"):]

        # Split into parts
        parts = path_part.split("/")

        # Expected: users/{user}/{area}/{category?}/{file?}
        if len(parts) < 3:
            return ParsedURI(
                scheme=PFAS_SCHEME,
                user_namespace=parts[1] if len(parts) > 1 else "",
                area=parts[2] if len(parts) > 2 else "",
                category=None,
                filename="",
                relative_path="",
            )

        user = parts[1]  # users/{user}
        area = parts[2]

        # Determine category and filename
        if len(parts) == 3:
            category = None
            filename = ""
            relative_path = ""
        elif len(parts) == 4:
            # Could be category/ or just a file
            if "." in parts[3]:
                category = None
                filename = parts[3]
                relative_path = filename
            else:
                category = parts[3]
                filename = ""
                relative_path = category
        else:
            category = parts[3]
            filename = parts[-1]
            relative_path = "/".join(parts[3:])

        return ParsedURI(
            scheme=PFAS_SCHEME,
            user_namespace=user,
            area=area,
            category=category,
            filename=filename,
            relative_path=relative_path,
        )

    def get_user_namespace(self, path: Path) -> Optional[str]:
        """
        Extract user namespace from path.

        Args:
            path: File path

        Returns:
            User namespace (lowercase) or None
        """
        try:
            relative = Path(path).resolve().relative_to(self.data_root)
            parts = relative.parts
            if len(parts) >= 2 and parts[0] == self.users_base:
                return parts[1].lower()
        except (ValueError, IndexError):
            pass
        return None

    def get_relative_path(self, path: Path) -> Optional[str]:
        """
        Get relative path from user directory.

        Args:
            path: Absolute file path

        Returns:
            Relative path from user dir or None
        """
        try:
            absolute = Path(path).resolve()
            relative = absolute.relative_to(self.data_root / self.users_base)
            parts = relative.parts

            if len(parts) >= 2:
                # Skip user name, return rest
                return "/".join(parts[1:])
        except (ValueError, IndexError):
            pass
        return None

    def normalize_uri(self, uri: str) -> str:
        """
        Normalize URI to canonical form.

        Args:
            uri: Input URI

        Returns:
            Normalized URI with lowercase paths
        """
        parsed = self.parse(uri)
        if not parsed.is_valid:
            return uri

        # Rebuild with normalized components
        normalized = f"{PFAS_SCHEME}://users/{parsed.user_namespace.lower()}/{parsed.area.lower()}"

        if parsed.relative_path:
            # Lowercase path except filename
            parts = parsed.relative_path.split("/")
            if len(parts) > 1:
                normalized_parts = [p.lower() for p in parts[:-1]] + [parts[-1]]
                normalized = f"{normalized}/{'/'.join(normalized_parts)}"
            else:
                normalized = f"{normalized}/{parsed.relative_path}"

        return normalized

    def is_valid_uri(self, uri: str) -> bool:
        """Check if string is a valid PFAS URI."""
        return self.parse(uri).is_valid

    def uri_exists(self, uri: str) -> bool:
        """Check if file at URI exists."""
        try:
            path = self.resolve(uri)
            return path.exists()
        except URIResolutionError:
            return False

    def list_user_namespaces(self) -> list:
        """List all available user namespaces."""
        users_path = self.data_root / self.users_base
        if not users_path.exists():
            return []

        return [
            d.name.lower()
            for d in users_path.iterdir()
            if d.is_dir()
        ]


def create_uri(
    user: str,
    area: str,
    category: Optional[str] = None,
    filename: Optional[str] = None
) -> str:
    """
    Create a PFAS URI from components.

    Args:
        user: User name
        area: Area (inbox, archive, config, reports)
        category: Optional category
        filename: Optional filename

    Returns:
        PFAS URI string
    """
    uri = f"{PFAS_SCHEME}://users/{user.lower()}/{area.lower()}"

    if category:
        uri = f"{uri}/{category.lower()}"

    if filename:
        uri = f"{uri}/{filename}"

    return uri

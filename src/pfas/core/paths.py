from pathlib import Path
import json
from datetime import date
from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from pfas.core.preferences import UserPreferences


class PathResolver:
    """Centralized, config-driven path resolver for PFAS.
    Makes all file paths configurable and future-proof.
    """
    def __init__(self, root_path: str | Path, user_name: str):
        self.root = Path(root_path).resolve()
        self.user_name = user_name.strip()

        # Load central config (fallback to sane defaults)
        self.config = self._load_config()

        # Use users_base from config (default "Users" to match actual filesystem)
        users_base = self.config.get("users_base", "Users")
        self.user_dir = self.root / users_base / self.user_name

    def _load_config(self) -> dict:
        config_path = self.root / "config" / "paths.json"
        if config_path.exists():
            with open(config_path, encoding="utf-8") as f:
                return json.load(f)
        # Fallback defaults if config missing
        return {
            "users_base": "Users",
            "per_user": {
                "db_file": "db/finance.db",
                "user_config_dir": "config",
                "inbox": "inbox",
                "archive": "archive",
                "reports": "reports"
            },
            "report_naming": {
                "pattern": "{user}_{asset}_{report_type}_{date}[_v{version}].xlsx",
                "date_format": "YYYY-MM-DD"
            }
        }

    def db_path(self) -> Path:
        return self.user_dir / self.config["per_user"]["db_file"]

    def user_config_dir(self) -> Path:
        return self.user_dir / self.config["per_user"]["user_config_dir"]

    def inbox(self) -> Path:
        return self.user_dir / self.config["per_user"]["inbox"]

    def archive(self) -> Path:
        return self.user_dir / self.config["per_user"]["archive"]

    def reports(self) -> Path:
        return self.user_dir / self.config["per_user"]["reports"]

    def user_config_file(self, filename: str) -> Optional[Path]:
        path = self.user_config_dir() / filename
        return path if path.exists() else None

    def global_config_file(self, filename: str) -> Path:
        return self.root / self.config["global"]["config_dir"] / filename

    def report_file(
        self,
        asset_type: str,
        report_type: str,
        version: str = "",
        extension: str = "xlsx"
    ) -> Path:
        """Generate standardized report filename."""
        today = date.today().strftime("%Y-%m-%d")
        pattern = self.config["report_naming"]["pattern"]
        name = pattern.format(
            user=self.user_name,
            asset=asset_type,
            report_type=report_type,
            date=today,
            version=version
        )
        # Remove optional [_v{version}] if empty
        name = name.replace("[_v{version}]", "").replace("[]", "")
        return self.reports() / asset_type / f"{name}.{extension}"

    def archive_file(self, original_filename: str) -> Path:
        """Generate timestamped archive name for original input file."""
        today = date.today().strftime("%Y-%m-%d")
        stem = Path(original_filename).stem
        ext = Path(original_filename).suffix
        return self.archive() / f"{today}_{self.user_name}_{stem}{ext}"

    def password_config_file(self) -> Path:
        """Get path to passwords.json configuration file."""
        return self.user_config_dir() / "passwords.json"

    def get_file_password(self, file_path: Path, interactive: bool = True) -> Optional[str]:
        """
        Get password for an encrypted file.

        Priority order:
        1. Exact filename match in passwords.json
        2. Pattern match in passwords.json
        3. Interactive prompt (if interactive=True)
        4. None

        Args:
            file_path: Path to the encrypted file
            interactive: Allow interactive password prompt

        Returns:
            Password string or None
        """
        pwd_file = self.password_config_file()

        if pwd_file.exists():
            try:
                with open(pwd_file, encoding='utf-8') as f:
                    data = json.load(f)

                # 1. Exact filename match (highest priority)
                filename = file_path.name
                if filename in data.get("files", {}):
                    return data["files"][filename]

                # 2. Pattern matching
                for pattern, pwd in data.get("patterns", {}).items():
                    if pattern == "*":
                        # Wildcard - use as last resort
                        continue
                    elif pattern.startswith("*."):
                        # Extension pattern like "*.pdf"
                        if filename.endswith(pattern[1:]):
                            return pwd
                    elif pattern in filename:
                        # Substring match
                        return pwd

                # 3. Wildcard fallback
                if "*" in data.get("patterns", {}):
                    return data["patterns"]["*"]

            except Exception as e:
                import logging
                logging.warning(f"Failed to read password config: {e}")

        # 4. Interactive prompt
        if interactive:
            return self._prompt_for_password(file_path)

        return None

    def _prompt_for_password(self, file_path: Path) -> Optional[str]:
        """
        Prompt user for password interactively.

        Args:
            file_path: Path to the encrypted file

        Returns:
            Password string or None
        """
        from getpass import getpass
        print(f"\nPassword required for: {file_path.name}")
        pwd = getpass("Enter password (hidden): ").strip()
        return pwd if pwd else None

    def get_preferences(self) -> "UserPreferences":
        """
        Load user preferences with fallback to defaults.

        Returns:
            UserPreferences instance
        """
        from pfas.core.preferences import UserPreferences
        global_config = self.root / self.config.get("global", {}).get("config_dir", "config")
        return UserPreferences.load(self.user_config_dir(), global_config)

    def get_report_path(
        self,
        report_type: str,
        fy: str,
        fmt: Optional[str] = None
    ) -> Path:
        """
        Get report output path using user preferences.

        Args:
            report_type: Type of report (balance_sheet, cash_flow, etc.)
            fy: Financial year (e.g., "2024-25")
            fmt: Output format (xlsx, pdf, json). If None, uses user's default.

        Returns:
            Full path for the report file (under user's reports directory)
        """
        prefs = self.get_preferences()
        if fmt is None:
            formats = prefs.reports.get_formats(report_type)
            fmt = formats[0] if formats else prefs.reports.default_format

        return prefs.get_report_output_path(self.reports(), report_type, fy, fmt)

    def ensure_user_structure(self) -> None:
        """
        Ensure user directory structure exists.

        Creates:
        - inbox/
        - archive/
        - reports/
        - config/
        - db/
        """
        directories = [
            self.inbox(),
            self.archive(),
            self.reports(),
            self.user_config_dir(),
            self.db_path().parent
        ]
        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)
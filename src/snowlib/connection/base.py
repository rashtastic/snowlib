"""Base connector class with shared profile and authentication logic."""

import os
from pathlib import Path
from typing import Optional, Any, Dict
from pydantic import SecretStr
import keyring
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

from .profiles import load_profile


class BaseConnector:
    """Base class for Snowflake connectors with TOML profile support and authentication handling"""
    
    def __init__(self, profile: str, **kwargs: Any) -> None:
        """Initialize the connector with a configuration profile and optional parameter overrides"""
        self.password: Optional[str] = None
        self.private_key: Optional[Any] = None

        self._cfg: Dict[str, Any] = load_profile(profile)
        self._cfg.update(kwargs)
        self._profile = profile
        self._process_auth()

    def _process_auth(self) -> None:
        """Process authentication credentials from the profile for keypair or password authentication"""
        auth = self._cfg.get("authenticator", "").upper()
        
        if auth == "SNOWFLAKE_JWT":
            self._process_keypair_auth()
        elif "password" in self._cfg:
            self.password = self._cfg["password"]

    def _process_keypair_auth(self) -> None:
        """Process keypair authentication by loading and deserializing the private key with optional passphrase"""
        key_path, passphrase = self._get_key_details()
        
        if passphrase:
            self._cfg["private_key_file_pwd"] = passphrase.get_secret_value()
        
        try:
            with open(key_path, "rb") as key_file:
                p_key_bytes = key_file.read()

            self.private_key = serialization.load_pem_private_key(
                p_key_bytes,
                password=passphrase.get_secret_value().encode() if passphrase else None,
                backend=default_backend()
            )
        except Exception as e:
            raise IOError(f"Failed to read or decrypt private key from {key_path}: {e}") from e

    def _get_key_details(self) -> tuple[Path, Optional[SecretStr]]:
        """Validate key path and retrieve the private key passphrase from environment variable or keyring"""
        private_key_file = self._cfg.get("private_key_file")
        if not private_key_file:
            raise ValueError(
                "Keypair authentication requires 'private_key_file' in profile configuration"
            )
        
        key_path = Path(private_key_file).expanduser()
        
        # Only allow absolute paths or home directory expansion
        if not key_path.is_absolute():
            raise ValueError(
                f"Private key path must be absolute or use ~ for home directory. Got: {private_key_file}"
            )
        
        if not key_path.exists():
            raise FileNotFoundError(f"Private key file not found: {key_path}")
            
        # Passphrase retrieval logic
        passphrase: Optional[SecretStr] = None
        
        passphrase_env_var = self._cfg.get("private_key_passphrase_env")
        if passphrase_env_var:
            env_pass = os.environ.get(passphrase_env_var)
            if env_pass:
                passphrase = SecretStr(env_pass)

        use_keyring = self._cfg.get("use_keyring", False)
        keyring_service = self._cfg.get("keyring_service", f"snowlib.{self._profile}")
        keyring_username = self._cfg.get("keyring_username", self._cfg.get("user"))

        if not passphrase and use_keyring:
            if not keyring_username:
                raise ValueError(
                    "Keyring usage requires 'user' in profile or 'keyring_username' override."
                )
            
            keyring_pass = keyring.get_password(keyring_service, keyring_username)
            if keyring_pass:
                passphrase = SecretStr(keyring_pass)
                
        return key_path, passphrase

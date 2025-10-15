"""Base connector class with shared profile and authentication logic."""

import os
from pathlib import Path
from typing import Optional, Any, Dict
from pydantic import SecretStr
import keyring
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

from ..config import load_profile


class BaseConnector:
    """
    Base class for Snowflake connectors with TOML profile support.
    
    This class handles common functionality for both SnowflakeConnector and
    SnowparkConnector, including:
    - Loading connection profiles from TOML configuration
    - Merging runtime parameter overrides
    - Processing keypair authentication with secure passphrase handling
    
    Derived classes should implement their specific connection logic.
    
    Args:
        profile: Name of the profile to load from connections.toml
        **kwargs: Additional parameters to override profile settings
    """
    
    def __init__(self, profile: str, **kwargs: Any) -> None:
        """
        Initialize the connector with a configuration profile.
        
        Args:
            profile: Name of the connection profile to load
            **kwargs: Override any connection parameters from the profile
        """
        # Public attributes for processed authentication credentials
        self.password: Optional[str] = None
        self.private_key: Optional[Any] = None

        # Load configuration from TOML file
        self._cfg: Dict[str, Any] = load_profile(profile)
        
        # Allow runtime overrides
        self._cfg.update(kwargs)
        
        # Store profile name for debugging/repr
        self._profile = profile
        
        # Process authentication credentials
        self._process_auth()

    def _process_auth(self) -> None:
        """
        Processes authentication credentials from the profile.
        
        Handles key-pair authentication by loading the private key and
        handles password authentication by retrieving it from the config.
        """
        auth = self._cfg.get("authenticator", "").upper()
        
        if auth == "SNOWFLAKE_JWT":
            self._process_keypair_auth()
        elif "password" in self._cfg:
            self.password = self._cfg["password"]

    def _process_keypair_auth(self) -> None:
        """
        Process keypair authentication configuration.
        
        If authenticator is SNOWFLAKE_JWT and private_key_file is specified,
        retrieves the passphrase from configured sources and prepares parameters
        for the Snowflake connector.
        
        Supported passphrase sources (opt-in only):
        - Environment variable (if private_key_passphrase_env is set)
        - System keyring/Windows Credential Manager (if use_keyring or keyring_* params are set)
        - None (for unencrypted keys on encrypted drives)
        
        Security:
        - Only absolute paths or home directory (~) are allowed
        - Relative paths are rejected to prevent keys in version control
        - Direct passphrase in config is NOT supported
        
        Note: This method populates self.private_key with a deserialized
        key object from the cryptography library and adds the passphrase
        to self._cfg for the Snowflake connector.
        """
        key_path, passphrase = self._get_key_details()
        
        # If we found a passphrase, add it to config for the Snowflake connector
        if passphrase:
            self._cfg["private_key_file_pwd"] = passphrase.get_secret_value()
        
        # Read the private key bytes and deserialize for SQLAlchemy
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
        """
        Validates key path and retrieves the private key passphrase.
        
        Returns:
            A tuple containing the resolved Path object for the key file
            and a SecretStr with the passphrase (or None).
        """
        private_key_file = self._cfg.get("private_key_file")
        if not private_key_file:
            raise ValueError(
                "Keypair authentication requires 'private_key_file' in profile configuration"
            )
        
        # Resolve file path
        key_path = Path(private_key_file).expanduser()
        
        # Only allow absolute paths or home directory expansion
        if not key_path.is_absolute():
            raise ValueError(
                f"Private key path must be absolute or use ~ for home directory. Got: {private_key_file}"
            )
        
        if not key_path.exists():
            raise FileNotFoundError(f"Private key file not found: {key_path}")
            
        # --- Passphrase retrieval logic ---
        passphrase: Optional[SecretStr] = None
        
        # 1. Check environment variable
        passphrase_env_var = self._cfg.get("private_key_passphrase_env")
        if passphrase_env_var:
            env_pass = os.environ.get(passphrase_env_var)
            if env_pass:
                passphrase = SecretStr(env_pass)

        # 2. Check keyring (if env var not found or not set)
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

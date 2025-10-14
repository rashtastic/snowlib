"""Base connector class with shared profile and authentication logic."""

import os
from pathlib import Path
from typing import Optional, Any, Dict
from pydantic import SecretStr
import keyring

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
        # Load configuration from TOML file
        self._cfg: Dict[str, Any] = load_profile(profile)
        
        # Allow runtime overrides
        self._cfg.update(kwargs)
        
        # Store profile name for debugging/repr
        self._profile = profile
        
        # Process keypair authentication if specified
        self._process_keypair_auth()
    
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
        
        Note: Snowflake connector handles the actual key loading,
        we just provide private_key_file and private_key_file_pwd.
        """
        auth = self._cfg.get("authenticator", "").upper()
        
        if auth != "SNOWFLAKE_JWT":
            return
        
        private_key_file = self._cfg.get("private_key_file")
        if not private_key_file:
            raise ValueError(
                "Keypair authentication requires 'private_key_file' in profile configuration"
            )
        
        # Resolve file path
        key_path = Path(private_key_file).expanduser()
        
        # Only allow absolute paths or home directory expansion
        # Reject relative paths to prevent keys being stored in version control
        if not key_path.is_absolute():
            raise ValueError(
                f"Private key path must be absolute or use ~ for home directory.\n" +
                f"Got: {private_key_file}\n\n" +
                f"Relative paths are not supported to prevent accidentally committing " +
                f"keys to version control.\n\n" +
                f"Valid examples:\n" +
                f"  - Absolute path: K:/snow2.p8 or C:/Users/you/.snowflake/key.p8\n" +
                f"  - Home directory: ~/.snowflake/rsa_key.p8\n" +
                f"  - Environment variable: $HOME/.snowflake/rsa_key.p8"
            )
        
        if not key_path.exists():
            raise FileNotFoundError(
                f"Private key file not found: {key_path}"
            )
        
        # Update the path to the resolved absolute path
        self._cfg["private_key_file"] = str(key_path)
        
        # Get passphrase from configured sources
        passphrase = self._get_key_passphrase()
        
        # Set private_key_file_pwd for Snowflake connector
        if passphrase:
            self._cfg["private_key_file_pwd"] = passphrase.get_secret_value()
        
        # Remove our custom config keys that Snowflake doesn't understand
        self._cfg.pop("private_key_passphrase_env", None)
        self._cfg.pop("use_keyring", None)
        self._cfg.pop("keyring_service", None)
        self._cfg.pop("keyring_username", None)
    
    def _get_key_passphrase(self) -> Optional[SecretStr]:
        """
        Get the private key passphrase from configured sources.
        
        Priority order (only uses sources explicitly configured):
        1. Custom environment variable (if private_key_passphrase_env is set)
        2. System keyring (if use_keyring=true or keyring_* params are set)
        3. None (for unencrypted keys)
        
        Note: Direct passphrase in config is NOT supported for security reasons.
        Use environment variables or Windows Credential Manager instead.
        
        Returns:
            Passphrase as SecretStr, or None if not found/not configured
        """
        # 1. Custom environment variable (only if explicitly configured)
        if "private_key_passphrase_env" in self._cfg:
            env_var = self._cfg["private_key_passphrase_env"]
            pwd = os.getenv(env_var)
            if pwd:
                return SecretStr(pwd)
            else:
                raise ValueError(
                    f"Environment variable '{env_var}' specified in " +
                    f"'private_key_passphrase_env' but not found in environment"
                )
        
        # 2. System keyring (only if explicitly configured)
        use_keyring = self._cfg.get("use_keyring", False)
        keyring_service = self._cfg.get("keyring_service")
        keyring_username = self._cfg.get("keyring_username")
        
        if use_keyring or keyring_service or keyring_username:
            # Both service and username are REQUIRED when using keyring
            if not keyring_service:
                raise ValueError(
                    "Keyring enabled but 'keyring_service' not specified in config. " +
                    "Example: keyring_service = 'snowflake'"
                )
            if not keyring_username:
                raise ValueError(
                    "Keyring enabled but 'keyring_username' not specified in config. " +
                    "Example: keyring_username = 'myuser_keypair'"
                )
            
            try:
                pwd = keyring.get_password(keyring_service, keyring_username)
                if pwd:
                    return SecretStr(pwd)
                else:
                    raise ValueError(
                        f"Keyring configured but no password found for " +
                        f"service='{keyring_service}', username='{keyring_username}'. " +
                        f"Store it with: keyring.set_password('{keyring_service}', '{keyring_username}', 'your-passphrase')"
                    )
            except Exception as e:
                raise ValueError(
                    f"Failed to retrieve passphrase from keyring " +
                    f"(service='{keyring_service}', username='{keyring_username}'): {e}"
                ) from e
        
        # 3. No passphrase configured (unencrypted key)
        return None

"""
Path validation and security utilities
Prevents directory traversal and validates file paths
"""
import os
import re
from pathlib import Path
from typing import Optional


def validate_and_create_path(base_dir: str, *subdirs) -> str:
    """
    Safely create and validate file paths
    Prevents directory traversal attacks
    
    Args:
        base_dir: Base directory path
        *subdirs: Subdirectory components
        
    Returns:
        Validated absolute path
        
    Raises:
        ValueError: If path is outside base directory
    """
    # Resolve to absolute path
    base = Path(base_dir).resolve()
    
    # Create full path
    full_path = base
    for subdir in subdirs:
        # Remove any potentially dangerous characters
        clean_subdir = sanitize_path_component(str(subdir))
        full_path = full_path / clean_subdir
    
    # Ensure path is within base directory (prevent directory traversal)
    try:
        full_path.resolve().relative_to(base.resolve())
    except ValueError:
        raise ValueError(f"Path {full_path} is outside base directory {base}")
    
    # Create directory if it doesn't exist
    full_path.parent.mkdir(parents=True, exist_ok=True)
    
    return str(full_path)


def sanitize_path_component(component: str) -> str:
    """
    Sanitize a single path component
    Removes dangerous characters while preserving valid ones
    
    Args:
        component: Path component to sanitize
        
    Returns:
        Sanitized path component
    """
    # Remove path traversal sequences
    component = component.replace("..", "")
    component = component.replace("./", "")
    component = component.replace("../", "")
    
    # Remove null bytes and other dangerous characters
    component = component.replace("\x00", "")
    component = component.replace("\r", "")
    component = component.replace("\n", "")
    
    # Remove leading/trailing slashes
    component = component.strip("/\\")
    
    return component


def sanitize_filename(filename: str, max_length: int = 255) -> str:
    """
    Sanitize a filename for safe file system usage
    
    Args:
        filename: Original filename
        max_length: Maximum allowed length
        
    Returns:
        Sanitized filename
    """
    # Remove directory separators
    filename = filename.replace("/", "_")
    filename = filename.replace("\\", "_")
    
    # Remove other dangerous characters
    filename = re.sub(r'[<>:"|?*\x00-\x1f]', "_", filename)
    
    # Remove leading/trailing dots and spaces
    filename = filename.strip(". ")
    
    # Ensure filename is not empty
    if not filename:
        filename = "unnamed"
    
    # Truncate if too long (leave room for extension)
    if len(filename) > max_length:
        name, ext = os.path.splitext(filename)
        max_name_length = max_length - len(ext)
        filename = name[:max_name_length] + ext
    
    return filename


def validate_subreddit_name(subreddit: str) -> bool:
    """
    Validate subreddit name format
    
    Args:
        subreddit: Subreddit name to validate
        
    Returns:
        True if valid, False otherwise
    """
    if not subreddit:
        return False
    
    # Subreddit names can only contain letters, numbers, and underscores
    # Must be 3-21 characters long
    pattern = r'^[a-zA-Z0-9_]{3,21}$'
    return bool(re.match(pattern, subreddit))


def validate_post_id(post_id: str) -> bool:
    """
    Validate Reddit post ID format
    
    Args:
        post_id: Post ID to validate
        
    Returns:
        True if valid, False otherwise
    """
    if not post_id:
        return False
    
    # Reddit post IDs are base36 encoded (lowercase letters and numbers)
    # Usually 5-7 characters long
    pattern = r'^[a-z0-9]{3,10}$'
    return bool(re.match(pattern, post_id))


def get_safe_data_path(date_str: str, filename: str) -> str:
    """
    Get a safe path for data files with date organization
    
    Args:
        date_str: Date string (YYYY-MM-DD format)
        filename: Filename to save
        
    Returns:
        Safe absolute path
    """
    # Validate date format
    if not re.match(r'^\d{4}-\d{2}-\d{2}$', date_str):
        raise ValueError(f"Invalid date format: {date_str}")
    
    # Sanitize filename
    safe_filename = sanitize_filename(filename)
    
    # Create safe path
    return validate_and_create_path("/data/raw_parquet", date_str, safe_filename)


def ensure_directory_exists(path: str) -> str:
    """
    Ensure a directory exists, create if necessary
    
    Args:
        path: Directory path
        
    Returns:
        Absolute path to directory
    """
    abs_path = os.path.abspath(path)
    os.makedirs(abs_path, exist_ok=True)
    return abs_path
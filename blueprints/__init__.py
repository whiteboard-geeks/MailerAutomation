"""
Blueprint package for MailerAutomation Flask application.
This package contains modules for different parts of the application.
"""

from blueprints.instantly import instantly_bp
from blueprints.easypost import easypost_bp
from blueprints.gmail import gmail_bp

# Export blueprints at package level
__all__ = ['instantly_bp', 'easypost_bp', 'gmail_bp']

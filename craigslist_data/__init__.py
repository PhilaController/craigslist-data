from importlib.metadata import version

# Set the version properly
__version__ = version(__package__)

# The app name on AWS
APP_NAME = __package__.replace("_", "-")

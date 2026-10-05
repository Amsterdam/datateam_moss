"""Logging module for the OpenTelemetry integration that sends logs to log analytics workspace
and adds a console logger for immediate feedback during development.
"""
import sys
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession
import logging
from azure.monitor.opentelemetry.exporter import AzureMonitorLogExporter
from opentelemetry.sdk._logs import (
    LoggerProvider,
    LoggingHandler,
)
from opentelemetry._logs import set_logger_provider, get_logger_provider
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor

spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)

def get_logger(name: str,
               stream_log_format: str = '%(asctime)s - %(levelname)s - %(message)s',
               azure_log_format: str = '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
               app_insights_connection_secret: str = 'app-insights-connection') -> logging.Logger:
    """Create and configure a logger with:
    - a `StreamHandler` that writes to `stdout`/notebook outputs using `stream_log_format`
    - an Azure Monitor exporter, initialized once globally, using a connection string retrieved from Azure Key Vault.
    - an `LoggingHandler` that forwards logs to Azure Monitor using `azure_log_format` (level `WARN`). Handlers are added only if they
      are not already present to avoid duplicate log entries.

    Parameters
    ----------
    name : str
        Name of the logger.
    stream_log_format : str, optional
        Format for console log messages. Defaults to '%(asctime)s - %(levelname)s - %(message)s'.
    azure_log_format : str, optional
        Format for Azure Monitor log messages. Defaults to '%(name)s - %(levelname)s - %(message)s'.
    app_insights_secret : str, optional
        Secret name in Key Vault that holds the Azure Monitor connection string.

    Returns
    -------
    logging.Logger
        The configured logger instance.
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.propagate = False  # prevent duplicate logs from root logger

    # --- Set StreamHandler ---
    if not any(isinstance(h, logging.StreamHandler) for h in logger.handlers):
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setLevel(logging.INFO)
        stream_handler.setFormatter(logging.Formatter(stream_log_format))
        logger.addHandler(stream_handler)

    # --- Initialize Azure Monitor backend ONCE globally ---
    current_provider = get_logger_provider()
    if not isinstance(current_provider, LoggerProvider):
        try:
            connection_string = dbutils.secrets.get(
                scope='keyvault', key=app_insights_connection_secret)
        except Exception as e:
            logger.error(f"Failed to retrieve secret: {e}")
            connection_string = None

        if connection_string:
            try:
                exporter = AzureMonitorLogExporter(
                    connection_string=connection_string)
                logger_provider = LoggerProvider()
                logger_provider.add_log_record_processor(
                    BatchLogRecordProcessor(exporter))
                set_logger_provider(logger_provider)
            except Exception as e:
                logger.error(f"Failed to initialize Azure Monitor exporter: {e}")

        else:
            logger.error(
                "AzureMonitorLogExporter not initialized due to missing connection string.")

    # --- Attach Azure handler to this logger (only once) ---
    if not any(isinstance(h, LoggingHandler) for h in logger.handlers):
        try:
            azure_handler = LoggingHandler()
            azure_handler.setLevel(logging.WARN)
            azure_handler.setFormatter(logging.Formatter(azure_log_format))
            logger.addHandler(azure_handler)
        except Exception as e:
            logger.warning(f"Failed to attach Azure LoggingHandler: {e}")

    return logger
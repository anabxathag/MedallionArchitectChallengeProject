import threading
import io

class ParallelLogger:
    """
    A utility to capture and buffer logs for parallel execution.
    It prevents interleaving of log messages from multiple threads.
    """
    _local = threading.local()

    @classmethod
    def get_logger(cls, component_name):
        """Initializes a buffer for the current thread/component."""
        cls._local.buffer = io.StringIO()
        cls._local.name = component_name
        cls.log(f"--- LOG START: {component_name} ---")

    @classmethod
    def log(cls, message):
        """Append a message to the current thread's buffer."""
        if hasattr(cls._local, 'buffer'):
            cls._local.buffer.write(f"{message}\n")
        else:
            # Fallback for main thread or uninitialized threads
            print(f"[MAIN] {message}")

    @classmethod
    def flush(cls):
        """Prints the buffered logs to stdout at once."""
        if hasattr(cls._local, 'buffer'):
            cls.log(f"--- LOG END: {cls._local.name} ---")
            print(cls._local.buffer.getvalue())
            cls._local.buffer.close()
            del cls._local.buffer

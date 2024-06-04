from configparser import ConfigParser

from tastytrade.session import ProductionSession


class ApplicationSession:
    """
    It provides access to the  session object and reads configuration from a config file.
    """

    def __init__(self) -> None:
        config: ConfigParser = ConfigParser()
        config.read('config.ini')

        login: str = config['TASTY']['Login']
        password: str = config['TASTY']['Password']
        self._session: ProductionSession = ProductionSession(login, password)
        self.initialized: bool = True  # Mark as initialized

    @property
    def session(self) -> ProductionSession:
        """
        Get the  session object.

        Returns:
            Session: The  session object.
        """
        return self._session
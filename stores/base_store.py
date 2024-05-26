import abc


class BaseStore(abc.ABC):
    def __init__(self, *args, **kwargs):
        pass

    @abc.abstractmethod
    def reset(self):
        pass

    @abc.abstractmethod
    def store_document(self, data: dict, timestamp: str):
        pass

    @abc.abstractmethod
    def get_document(self, timestamp: str):
        pass

    @abc.abstractmethod
    def get_total_size(self):
        pass

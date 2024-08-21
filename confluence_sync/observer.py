import abc

from confluence_sync import events


class Observer(abc.ABC):
    def update(self, event: events.Event) -> None:
        """Выполнение действия на событие наблюдаемого объекта."""
        raise NotImplementedError


class Observable:
    def __init__(self) -> None:
        self._observers: list['Observer'] = []

    def notify(self, event: events.Event) -> None:
        """Уведомление наблюдателей."""
        for observer in self._observers:
            observer.update(event)

    def attach(self, observer: Observer) -> None:
        """Добавление наблюдателя."""
        if observer not in self._observers:
            self._observers.append(observer)

    def detach(self, observer: Observer) -> None:
        """Удаление наблюдателя."""
        try:
            self._observers.remove(observer)
        except ValueError:
            pass

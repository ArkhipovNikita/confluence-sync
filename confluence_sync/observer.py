import abc

from confluence_sync import events


class Observer(abc.ABC):
	def update(self, event: events.Event) -> None:
		"""Do something when the observed object triggers an event."""
		raise NotImplementedError


class Observable:
	def __init__(self) -> None:
		self._observers: list['Observer'] = []

	def notify(self, event: events.Event) -> None:
		"""Notify the observers."""
		for observer in self._observers:
			observer.update(event)

	def attach(self, observer: Observer) -> None:
		"""Add an observer."""
		if observer not in self._observers:
			self._observers.append(observer)

	def detach(self, observer: Observer) -> None:
		"""Remove an observer."""
		try:
			self._observers.remove(observer)
		except ValueError:
			pass

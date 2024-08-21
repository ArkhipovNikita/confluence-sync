import queue
import typing as tp

NodeT = tp.TypeVar('NodeT', bound=tp.Any)


class Node(tp.Generic[NodeT]):
    """First child / next sibling."""

    def __init__(self, data: NodeT | None = None) -> None:
        self.data = data

        self._next = None
        self._next_tail: 'Node[NodeT]' | None = None

        self._sibling: 'Node[NodeT]' | None = None

    @property
    def descendants_count(self) -> int:
        cnt = 0

        for _ in self.descendants():
            cnt += 1

        return cnt

    def add_child(self, node: 'Node[NodeT]') -> None:
        if self._next_tail:
            self._next_tail._sibling = node
            self._next_tail = self._next_tail._sibling
        else:
            self._next = node
            self._next_tail = self._next

    # type hint через pipe вызывает ошибку
    def find_child_by(self, key: tp.Callable[[NodeT], bool]) -> tp.Optional['Node[NodeT]']:
        for child in self.children():
            if key(child.data):
                return child

    def find_descendant_by(self, key: tp.Callable[[NodeT], bool]) -> tp.Optional['Node[NodeT]']:
        for descendant in self.descendants():
            if key(descendant.data):
                return descendant

    def descendants(self) -> tp.Iterator['Node[NodeT]']:
        nodes = queue.SimpleQueue()
        nodes.put(self)

        while not nodes.empty():
            node = nodes.get()

            for child in node.children():
                yield child

                nodes.put(child)

    def children(self) -> tp.Iterator['Node[NodeT]']:
        if self._next:
            yield self._next
        else:
            return

        node = self._next._sibling

        while node:
            yield node
            node = node._sibling

    def __repr__(self) -> str:
        return str(self.data)

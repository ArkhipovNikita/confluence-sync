import io


class Replacer:
    """Класс для модификации текста."""

    def __init__(self, text: str) -> None:
        # Используется `io.StringIO` для более простого интерфейса чтения / записи
        self._in_body = io.StringIO(text)
        self._out_body = io.StringIO()

        self._prev_pos = 0
        self._prev_lineno = 0

    def __enter__(self) -> 'Replacer':
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._in_body.close()
        self._out_body.close()

    def getvalue(self) -> str:
        self._out_body.write(self._in_body.read())
        return self._out_body.getvalue()

    def replace(self, lineno: int, pos: int, length: int, text: str) -> None:
        """Замена части текста.

        :param lineno: текущий номер строки
        :param pos: текущий индекс начала заменяемой подстроки
        :param length: длина заменяемого текста
        :param text: новый текст
        """
        # пропуск строк
        skip_lines = lineno - self._prev_lineno

        for _ in range(skip_lines):
            self._out_body.write(self._in_body.readline())

        # если строка поменялась, то позиция на строке должна быть на начале
        self._prev_pos = self._prev_pos if skip_lines == 0 else 0

        # запись остатки строки и нового текста
        self._out_body.write(self._in_body.read(pos - self._prev_pos))
        self._out_body.write(text)

        # пропуск замененной строки
        # (relative seek возможен только в бинарном режиме, поэтому используется read)
        self._in_body.read(length + 1)

        self._prev_pos = pos + length + 1
        self._prev_lineno = lineno

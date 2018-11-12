from abc import ABC, abstractmethod
from io import TextIOWrapper, BufferedWriter
from time import sleep
from typing import Tuple, Any, Optional
from pickle import Pickler


WRITE_ROW_ARTIFICIAL_SLEEP = 1  # longer than Measurer ping timeout is dangerous


class DatafileWriter(ABC):
    """Defines actual file writers interface

    One should start_new_file, then set_column_names, then write_header,
    and then write_row as many times as needed. When done, one should be able
    to repeat this sequence without problems.

    IMPLEMENT THIS INTERFACE SUCH THAT THIS STATEMACHINE STUFF IS TAKEN CARE
    OF AUTOMAGICALLY
    """
    @abstractmethod
    def start_new_file(self, new_file_name: str):
        pass

    @abstractmethod
    def set_column_names(self, columns: Tuple[str]):
        pass

    @abstractmethod
    def write_header(self):
        pass

    @abstractmethod
    def write_row(self, datatuple: Tuple[Tuple[str, Any]]):
        pass


class GnuplotWriter(DatafileWriter):
    """
    MAKE THIS CLASS OPEN A FILE UPON ITS INITIALIZATION IN ORDER TO REMOVE
    UNNECESSARY STATE MANAGEMENT RELATED TO filehandle
    """
    _extension = '.dat'

    def __init__(self):
        self._filename: str = ''
        self._filehandle: Optional[TextIOWrapper] = None

        # the ORDERED column names of the data
        self._columns: Optional[Tuple[str]] = None

    def start_new_file(self, new_file_name: str):
        self._close_file()
        self._open_file(new_file_name)

    def set_column_names(self, columns: Tuple[str]):
        self._columns = columns

    def write_header(self):
        """
        Write the gnuplot header
        """
        line = ' '.join(self._columns) + '\n'
        self._write_and_flush(line)

    def write_row(self, datatuple: Tuple[Tuple[str, Any]]):
        """
        Append a row to a gnuplot file. For now only handles single points

        The result tuple must have the correct size.
        Nulls are needed where data is missing
        """
        # to ensure writing the correct number in the correct column,
        # we must sort the input
        sorted_data = sorted(datatuple, key=lambda x: self._columns.index(x[0]))
        datapoints = tuple(tup[1] for tup in sorted_data)

        line = " ".join([str(datum) for datum in datapoints]) + "\n"
        self._write_and_flush(line)

        # sleeping longer than the Measurer ping timeout is dangerous
        sleep(WRITE_ROW_ARTIFICIAL_SLEEP)

    def _open_file(self, filename):
        if self._filehandle is not None:
            raise Exception(f'There is already an open file ({self._filename})')

        if not filename.endswith(self._extension):
            filename = filename + self._extension

        self._filename = filename
        self._filehandle = open(self._filename, 'a')

    def _write_and_flush(self, line):
        self._filehandle.write(line)
        self._filehandle.flush()

    def _close_file(self):
        if self._filehandle is not None:
            self._filehandle.flush()
            self._filehandle.close()
            self._filehandle = None

            self._filename = ''
            self._columns = None

    def __del__(self):
        self._close_file()


class PickleWriter(DatafileWriter):
    """

    """
    _extension = '.pkl'

    def __init__(self):
        self._filename: str = ''
        self._filehandle: Optional[BufferedWriter] = None
        self._pickler: Optional[Pickler] = None

    def start_new_file(self, new_file_name: str):
        self._close_file()
        self._open_file(new_file_name)

    def set_column_names(self, columns: Tuple[str]):
        pass

    def write_header(self):
        """
        there is no header
        """
        pass

    def write_row(self, datatuple: Tuple[Tuple[str, Any]]):
        """
        Append a row to a gnuplot file. For now only handles single points

        The result tuple must have the correct size.
        Nulls are needed where data is missing
        """
        # to ensure writing the correct number in the correct column,
        # we must sort the input
        self._pickler.dump(datatuple)
        # sleeping longer than the Measurer ping timeout is dangerous
        sleep(WRITE_ROW_ARTIFICIAL_SLEEP)

    def _open_file(self, filename):
        if self._filehandle is not None:
            raise Exception(
                f'There is already an open file ({self._filename})')

        if not filename.endswith(self._extension):
            filename = filename + self._extension

        self._filename = filename
        self._filehandle = open(self._filename, 'ba')
        self._pickler = Pickler(self._filehandle)

    def _close_file(self):
        if self._filehandle is not None:
            self._filehandle.flush()
            self._filehandle.close()
            self._filehandle = None

            self._filename = ''
            self._columns = None

        if self._pickler is not None:
            del self._pickler
            self._pickler = None

    def __del__(self):
        self._close_file()

from avro.datafile import DataFileReader
from avro.io import DatumReader

from pyspark.serializer import Serializer

class AvroDeserializer(Serializer):
    """
    Deserializes streams written by Avro
    """

    def loads(self, stream):
        with DataFileReader(stream, DatumReader()) as records:
            for record in records:
                yield record

    def load_stream(self, stream):
        while True:
            try:
                yield self.loads(stream)
            except EOFError:
                return
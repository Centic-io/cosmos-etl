from database.mongodb import MongodbItemExporter


def create_streaming_exporter(output, collector_id, db_prefix=""):
    if not collector_id:
        return None
    streaming_exporter_type = determine_item_exporter_type(output)
    if streaming_exporter_type == StreamingExporterType.MONGODB:
        streaming_exporter = MongodbItemExporter(
            connection_url=output, collector_id=collector_id, db_prefix=db_prefix)
    else:
        streaming_exporter = None
    return streaming_exporter


def determine_item_exporter_type(output):
    if output is not None and output.startswith('mongodb'):
        return StreamingExporterType.MONGODB
    else:
        return StreamingExporterType.UNKNOWN


class StreamingExporterType:
    MONGODB = 'mongodb'
    UNKNOWN = 'unknown'

from p4.config.v1.p4info_pb2 import P4Info


class ElementsP4Info:
    """ElementsP4Info.

    This class is responsible for indexing P4Info objects by a given attribute.
    By default it'll index objects by their fully qualified name.
    """

    def __init__(self, p4info: P4Info, iter_attr="name") -> None:
        """ElementsP4Info."""
        self.p4info: P4Info = p4info

        self.tables = self._index_by_preamble("tables", iter_attr)
        self.actions = self._index_by_preamble("actions", iter_attr)
        self.action_profiles = self._index_by_preamble("action_profiles", iter_attr)
        self.counters = self._index_by_preamble("counters", iter_attr)
        self.direct_counters = self._index_by_preamble("direct_counters", iter_attr)
        self.meters = self._index_by_preamble("meters", iter_attr)
        self.direct_meters = self._index_by_preamble("direct_meters", iter_attr)
        self.value_sets = self._index_by_preamble("value_sets", iter_attr)
        self.registers = self._index_by_preamble("digests", iter_attr)
        self.digests = self._index_by_preamble("digests", iter_attr)
        self.externs = self._index_by_preamble("externs", iter_attr)
        self.table_match_fields = self._index_by_table_match_fields(iter_attr)

    def _index_by_table_match_fields(self, iter_attr="name") -> dict:
        table_match_fields = {}
        for table_name, table in self.tables.items():
            for match_field in table.match_fields:
                key = (table_name, getattr(match_field, iter_attr))
                table_match_fields[key] = match_field
        return table_match_fields

    def _index_by_preamble(self, iter_name: str, iter_attr="name") -> dict:
        return {
            getattr(item.preamble, iter_attr): item
            for item in getattr(self.p4info, iter_name, [])
        }

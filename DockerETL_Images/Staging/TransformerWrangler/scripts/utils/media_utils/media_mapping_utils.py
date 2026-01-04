import uuid


class MediaMappingUtils:

    def createConnectionKey(row, pk_attributes):
        return "".join(str(row[pk_att]).lower() for pk_att in pk_attributes)

    def map_distinct_value(new_row_to_add, distinct_value_set, pk_attributes):
        # new_row[pk_attributes] is the string_key that mixes all differenciating primary key values
        ref_id = new_row_to_add["ref_id"]
        del new_row_to_add["ref_id"]
        # create a key so that for each pk_att: key = "lower(new_data[pk_att[0]])lower(new_data[pk_att[1]])..."
        distinct_pk_key = MediaMappingUtils.createConnectionKey(
            new_row_to_add, pk_attributes
        )
        if distinct_pk_key in distinct_value_set:
            distinct_value_set[distinct_pk_key]["refs"].append(ref_id)
        else:
            distinct_value_set[distinct_pk_key] = {
                "id": uuid.uuid4(),
                "refs": [ref_id],
            } | new_row_to_add

    def map_distinct_values(new_rows_to_add, distinct_value_set, pk_attributes):
        for new_row in new_rows_to_add:
            MediaMappingUtils.map_distinct_value(
                new_row, distinct_value_set, pk_attributes
            )

    def remap_foreign_keys_and_build_distinct_rows(
        main_rows, distinct_value_set, foreign_key_attribute
    ):
        distinct_rows = []
        for distinct_row in distinct_value_set.values():
            for ref_id in distinct_row["refs"]:
                foreign_key = distinct_row["id"]
                if main_rows[ref_id][foreign_key_attribute] is None:
                    main_rows[ref_id][foreign_key_attribute] = foreign_key
                else:
                    main_rows[ref_id][foreign_key_attribute].append(foreign_key)
            del distinct_row["refs"]
            distinct_rows.append(distinct_row)
        return distinct_rows

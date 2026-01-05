import uuid
from utils.logger import LOG


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
                "id": str(uuid.uuid4()),
                "refs": [ref_id],
            } | new_row_to_add

    def map_distinct_values(new_rows_to_add, distinct_value_set, pk_attributes):
        for new_row in new_rows_to_add:
            MediaMappingUtils.map_distinct_value(
                new_row, distinct_value_set, pk_attributes
            )

    def remap_foreign_keys_and_build_distinct_rows(
        main_rows,
        distinct_value_set,
        foreign_key_attribute,
        # null_check_collums=[],
        # value_map=None,
    ):
        distinct_rows = []
        for distinct_row in distinct_value_set.values():
            # valid_row = True
            # for col in null_check_collums:
            #     valid_row = distinct_row[col] is not None
            #     if not valid_row:
            #         break
            # if not valid_row:
            #     continue

            for ref_id in distinct_row["refs"]:
                foreign_key = distinct_row["id"]

                if main_rows[ref_id][foreign_key_attribute] is None:
                    main_rows[ref_id][foreign_key_attribute] = foreign_key
                # elif valid_row:
                else:
                    main_rows[ref_id][foreign_key_attribute].append(foreign_key)
            del distinct_row["refs"]
            distinct_rows.append(distinct_row)
        return distinct_rows

    def map_best_candidate_to_target_title(candidates, result, best_found):

        # candidate: row of imdb merged with possible metacritic_title and similarity
        # result: tconst -> metacritic_title
        # best_similarity_found: metacritic_title_id -> [max_similarity_found_for_him, tconst]

        matches_c = 0
        for row in candidates.itertuples():
            tconst = row.tconst
            target_id = row.target_id
            similarity = row.similarity

            # Check if target_id already has a better match
            if (
                target_id not in best_found
                or best_found[target_id]["similarity"] < similarity
            ):

                # If already assigned and indeed a better match
                if target_id in best_found:
                    old_tconst = best_found[target_id]["associated"]
                    LOG.info(
                        f"Deleting previous assignment: {row.target_title} -> {best_found[target_id]["titleIMDB"]} ({best_found[target_id]["similarity"]})"
                    )
                    matches_c -= 1
                    del result[old_tconst]

                # Check if imdb title already assigned somewhere else (uniqueness of IMDB_title matching)
                if tconst in result:
                    old_target = result[tconst]
                    old_similarity = best_found[old_target]["similarity"]
                    if similarity <= old_similarity:
                        continue  # skip, this match is worse than existing
                    else:
                        # remove old worse assignment
                        LOG.info(
                            f"Deleting previous assignment: {row.primaryTitle} was assigned to "
                            f"{old_target}  with similarity {old_similarity}"
                        )
                        matches_c -= 1
                        del best_found[old_target]

                # Assign new match
                best_found[target_id] = {
                    "similarity": similarity,
                    "associated": tconst,
                    "titleIMDB": row.primaryTitle,
                }
                result[tconst] = target_id
                matches_c += 1
                LOG.info(
                    f"New King of the Hill: {row.target_title} -> {row.primaryTitle} ({similarity})"
                )
        return matches_c

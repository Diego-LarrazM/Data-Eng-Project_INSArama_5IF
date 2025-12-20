import json
import uuid
import pandas as pd
from pathlib import Path
# from sklearn.cluster import HDBSCAN
# from sklearn.metrics.pairwise import cosine_distances
# from sentence_transformers import SentenceTransformer


ROOT_DIR = Path(__file__).resolve().parent
DATA_DIR = ROOT_DIR / "data"
OUTPUT_DIR = ROOT_DIR / "output"
# IO


def load_json(path: Path) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def extract_title(data: dict) -> str | None:
    return data.get("title") or data.get("tv_title") or data.get("game_title")


# DIM MEDIA INFO


def build_mediainfo_rows(data, media_type):
    md = data.get("media_details", {})
    media_info_id = uuid.uuid4()
    genre_rows = [
        {"ref_id": media_info_id, "genre_title": g.strip()}
        for g in md.get("genres", [])
    ]
    companies = (
        [("developer", d) for d in md.get("developers", [])]
        + [("publisher", p) for p in md.get("publishers", [])]
        + [("production_companies", pc) for pc in md.get("production_companies", [])]
    )
    company_rows = [
        {"ref_id": media_info_id, "company_role": cr, "company_name": cn}
        for cr, cn in companies
    ]

    primary_title = extract_title(data)
    return (
        {
            media_info_id: {
                "primary_title": primary_title,
                "title_language": md.get("original_language"),
                "original_title": md.get("original_title"),
                "media_type": media_type,
                "duration": md.get("duration"),
                "description": md.get("summary"),
                "release_date": md.get("initial_release_date")
                or md.get("release_date"),
                "PEGI_MPA_Rating": md.get("rating"),
                "genre_id": [],
                "company_id": [],
            }
        },
        genre_rows,
        company_rows,
    )


# REVIEWS EXTRACTION


def extract_all_reviews(data):
    reviews = []

    for section, section_reviews in data.get("critic_reviews", {}).items():
        reviews.append((section, section_reviews))
    for section, section_reviews in data.get("user_reviews", {}).items():
        reviews.append((section, section_reviews))
    return reviews


# FACT REVIEWS (DFR)


def build_fact_rows(data, mediainfo_id, section_type):
    review_rows = {}
    time_rows = []
    reviewer_rows = []
    section_rows = []

    # ["section1", [review1, review2], "section2", [review3, review4], ...]

    for section, reviews in extract_all_reviews(data):
        for r in reviews:
            review_id = uuid.uuid4()
            # Append review
            review_rows |= {
                review_id: {
                    "time_id": None,
                    "section_id": None,
                    "reviewer_id": None,
                    "media_info_id": mediainfo_id,
                    "rating": r.get("rating"),
                }
            }
            # Append time
            post_date = r.get("post_date")
            if post_date:
                year, month, day = post_date.split("-")
            else:
                year, month, day = (None, None, None)
            time_rows.append(
                {"year": year, "month": month, "day": day, "ref_id": review_id}
            )
            # Append reviewer
            reviewer_rows.append(
                {
                    "association": r.get("company"),
                    "reviewer_username": r.get("author"),
                    "is_critic": r.get("isCritic"),
                    "ref_id": review_id,
                }
            )
            # Append platform
            section_rows.append(
                {
                    "section_type": section_type,
                    "section_name": section,
                    "ref_id": review_id,
                }
            )
    return review_rows, time_rows, reviewer_rows, section_rows


# ref_uuid is either media_info_id or review_id


def map_distinct_values(new_rows_to_add, distinct_value_set, pk_attributes):
    for new_row in new_rows_to_add:
        # new_row[pk_attributes] is the string_key that mixes all differenciating primary key values

        ref_id = new_row["ref_id"]
        del new_row["ref_id"]
        # create a key so that for each pk_att: key = "lower(new_data[pk_att[0]])lower(new_data[pk_att[1]])..."
        distinct_pk_key = "".join(
            str(new_row[pk_att]).lower() for pk_att in pk_attributes
        )
        if distinct_pk_key in distinct_value_set:
            distinct_value_set[distinct_pk_key]["refs"].append(ref_id)
        else:
            distinct_value_set[distinct_pk_key] = {
                "id": uuid.uuid4(),
                "refs": [ref_id],
            } | new_row


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


def build_bridge_rows(main_rows, foreign_key_titles, main_title):
    bridge_dfs = {fkt: [] for fkt in foreign_key_titles}
    for main_id, row in main_rows.items():
        for fk_title in foreign_key_titles:
            if len(row[fk_title]) == 0:
                del row[fk_title]
                continue
            fk_weight = 1 / len(row[fk_title])
            for fk in row[fk_title]:
                bridge_dfs[fk_title].append(
                    {main_title: main_id, f"{fk_title}": fk, "weight": fk_weight}
                )
            del row[fk_title]
    return bridge_dfs


def build_and_save_dataframe_from_rows(
    rows, output_filename = None, separator="|", id_attribute_names=["id"], is_dict=False
):
    if is_dict:
        # data = {
        #     idrow1: {"colA": valA, "colB": valB},
        #     idrow2: {"colA": valA2, "colB": valB2},
        # }
        df = pd.DataFrame.from_dict(rows, orient="index")
        df.index.names = id_attribute_names
    else:
        # rows = [
        #     {"id": 101, "age": 30, "score": 88},
        #     {"id": 102, "age": 25, "score": 92},
        # ]
        df = pd.DataFrame.from_records(rows, index=id_attribute_names)
    if(output_filename) : df.to_csv(OUTPUT_DIR / output_filename, sep=separator, encoding="utf-8")
    return df



# def get_embeddings_of(listOfElements):
#     model = SentenceTransformer('all-MiniLM-L6-v2')
#     return model.encode(listOfElements)

# def cluster_attribute_embedding(dataframe, attribute, output_label):
#     embeddings = get_embeddings_of(dataframe[attribute].tolist())
#     dist_matrix = cosine_distances(embeddings)
#     clusterer = HDBSCAN(
#         min_cluster_size=2,
#         metric='precomputed'
#     )
#     dataframe[output_label] = clusterer.fit_predict(dist_matrix)

def preprocess_title(title):
    import spacy
    nlp = spacy.load("en_core_web_sm", disable=["parser", "ner"])
    doc = nlp(title.lower())
    tokens = [token.lemma_ for token in doc if not token.is_stop and token.is_alpha]
    return set(tokens)

# Step 2: Compute pairwise Jaccard similarity
def jaccard(set1, set2):
    return len(set1 & set2) / len(set1 | set2)
stopwords = {"the", "of", "a", "an", "and", "in", "on", "at", "ii", "iii", "iv", "v", "vi","vii","viii", "ix", "x", "1", "2", "3", "4", "5"," 6", "7", "8", "9", "10", "100", "1000" "season", "part", "re", "release", "remastered"}

def cluster_attribute_jaccard(dataframe, attribute, output_label, type_attribute = None, threshold = 0.2, og_indexes = ["id"]):
    from itertools import combinations
    import networkx as nx

    def create_jaccard_similarity_graph(df, token_col, type_col, threshold):
        # Collect all pairs above threshold
        G = nx.Graph()
        G.add_nodes_from(df.index)
        # instead of titles we use sets of tokens (ex: {"super", "mario", "galaxy"}) and compare each other by combinations of 2
        for (i, t1), (j, t2) in combinations(enumerate(df[token_col]), 2): 
            # Skip if different media types
            if type_col and df[type_col].iloc[i] != df[type_col].iloc[j]:
                continue
            sim = len(t1 & t2) / len(t1 | t2)  # Jaccard similarity
            if sim >= threshold:
                G.add_edge(i, j)
        return G
    
    def find_group_label(df, attribute, cluster):
        """
        df: DataFrame
        token_col: column containing sets of tokens
        cluster: list of row indices in this cluster

        returns: string of sequential common
        """
        # Split title in words
        split_titles = [df[attribute].iloc[idx].split() for idx in cluster]
        # Get largest continuous common words pattern
        label_elements = []
        for words in zip(*split_titles):
            if all(w == words[0] for w in words):
                label_elements.append(words[0])
        return " ".join(label_elements)
    
    df = dataframe.reset_index(drop=False) # use positional index for efficiency
    df[output_label] = "Standalone"
    df["_tokens"] = df[attribute].apply(lambda t: set(t.lower().replace('.', '').split()) - stopwords)
    clusters_graph = create_jaccard_similarity_graph(df, "_tokens", type_attribute,  threshold)
    clusters = [c for c in nx.connected_components(clusters_graph) if len(c) > 1] # clusters = sets of nodes connected by edges [{node1, node2, ...}, {...}, ...]

    for cluster in clusters:
        group_label = find_group_label(df, attribute, cluster)
        if group_label:
            df.loc[list(cluster), output_label] = group_label # map for each element of cluster their group label

    df.drop(columns=["_tokens"], inplace=True)
    df.set_index(og_indexes, inplace=True) # restore indexes
    return df

    


    # Map titles to cluster IDs and Assign clean franchise labels
    # title_to_cluster = {}
    # cluster_names = {}
    # for cluster_id, cluster in enumerate(clusters):
    #     for title in cluster:
    #         title_to_cluster[title] = cluster_id

    # dataframe[output_label] = dataframe[attribute].map(title_to_cluster)

    # # Step 6: 
    # cluster_names = {}
    # for cluster_id, cluster in enumerate(clusters):
    #     # Collect all tokens in cluster
    #     cluster_tokens = [token for t in cluster for token in dataframe.loc[dataframe[attribute]==t, '_tokens'].values[0]]
    #     # Pick top N words
    #     most_common_words = [word for word, count in Counter(cluster_tokens).most_common(top_words)]
    #     cluster_names[cluster_id] = ' '.join(most_common_words).title()  # Capitalize

    # dataframe[output_label] = dataframe[output_label + "_id"].map(cluster_names)
    # pairs_df.to_csv(OUTPUT_DIR / "jaccard.csv", sep='|', encoding="utf-8")

def main():
    # Sets of distinct value : [list of uuids that use this value as a dim]
    genre_connection = {}
    company_connection = {}
    time_connection = {}
    reviewer_connection = {}
    section_connection = {}

    # All by default distinct rows
    media_rows = {}
    review_rows = {}

    for cat_dir in DATA_DIR.iterdir():
        if not cat_dir.is_dir():
            continue
        media_type = "Movie"
        section_type = "Display"
        if cat_dir.name == "games":
            section_type = "Platform"
            media_type = "Video Game"
        elif cat_dir.name == "tvshows":
            section_type = "Season"
            media_type = "TV Series"

        for jf in cat_dir.glob("*.json"):
            data = load_json(jf)
            to_add_media_info_row, to_add_genres, to_add_companies = (
                build_mediainfo_rows(data, media_type)
            )
            media_info_id = list(to_add_media_info_row.keys())[0]
            to_add_review_rows, to_add_timestamps, to_add_reviewers, to_add_sections = (
                build_fact_rows(data, media_info_id, section_type)
            )

            # Add new already known distinct rows
            media_rows |= to_add_media_info_row
            review_rows |= to_add_review_rows

            # Map new distinct values or add to already defined new rows that reference them to replace uuid later
            map_distinct_values(to_add_genres, genre_connection, ["genre_title"])
            map_distinct_values(
                to_add_companies, company_connection, ["company_name", "company_role"]
            )
            map_distinct_values(
                to_add_timestamps, time_connection, ["year", "month", "day"]
            )
            map_distinct_values(
                to_add_reviewers,
                reviewer_connection,
                ["reviewer_username", "association"],
            )
            map_distinct_values(
                to_add_sections, section_connection, ["section_name", "section_type"]
            )

    # Remapping
    genre_rows = remap_foreign_keys_and_build_distinct_rows(
        media_rows, genre_connection, "genre_id"
    )
    company_rows = remap_foreign_keys_and_build_distinct_rows(
        media_rows, company_connection, "company_id"
    )
    time_rows = remap_foreign_keys_and_build_distinct_rows(
        review_rows, time_connection, "time_id"
    )
    reviewer_rows = remap_foreign_keys_and_build_distinct_rows(
        review_rows, reviewer_connection, "reviewer_id"
    )
    section_rows = remap_foreign_keys_and_build_distinct_rows(
        review_rows, section_connection, "section_id"
    )

    # Build bridge tables for media_info
    bridge_dfs_media_info = build_bridge_rows(
        media_rows, ["genre_id", "company_id"], "media_id"
    )
    bridge_media_genre_rows = bridge_dfs_media_info["genre_id"]
    bridge_media_company_rows = bridge_dfs_media_info["company_id"]

    # DataFrames and CSVs
    media_df = build_and_save_dataframe_from_rows(media_rows, is_dict=True)
    media_df = cluster_attribute_jaccard(media_df, "primary_title", "franchise", type_attribute="media_type")
    media_df.to_csv(OUTPUT_DIR / "media_info.csv", sep='|', encoding="utf-8")

    build_and_save_dataframe_from_rows(
        bridge_media_genre_rows,
        "bridge_media_genre.csv",
        id_attribute_names=["media_id", "genre_id"],
    )

    build_and_save_dataframe_from_rows(
        bridge_media_company_rows,
        "bridge_media_company.csv",
        id_attribute_names=["media_id", "company_id"],
    )

    build_and_save_dataframe_from_rows(genre_rows, "genres.csv")

    build_and_save_dataframe_from_rows(company_rows, "companies.csv")

    build_and_save_dataframe_from_rows(review_rows, "reviews.csv", is_dict=True)

    build_and_save_dataframe_from_rows(time_rows, "time.csv")

    build_and_save_dataframe_from_rows(reviewer_rows, "reviewers.csv")

    build_and_save_dataframe_from_rows(section_rows, "sections.csv")


if __name__ == "__main__":
    main()

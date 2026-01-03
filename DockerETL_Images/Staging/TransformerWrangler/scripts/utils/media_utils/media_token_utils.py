from itertools import combinations
import networkx as nx
import pandas as pd
import re

STOPWORDS_FRANCHISE = {
    "the",
    "of",
    "a",
    "an",
    "and",
    "in",
    "on",
    "at",
    "ii",
    "iii",
    "iv",
    "v",
    "vi",
    "vii",
    "viii",
    "ix",
    "x",
    "1",
    "2",
    "3",
    "4",
    "5",
    "6",
    "7",
    "8",
    "9",
    "10",
    "season",
    "part",
    "re",
    "release",
    "remastered",
}

IMDB_TITLE_STOPWORDS = {"the", "a", "and", "as"}


class MediaTokenUtils:

    def could_be_same_media(
        expected_year: int,
        imdb_year: int,
        expected_runtime: int,
        imdb_runtime: str,
        acceptance_year_interval: int,
        acceptance_runtime_interval: int,
    ) -> bool:
        year_equivalence = False
        if imdb_year != "\\N" and expected_year is not None and imdb_year is not None:
            year_equivalence = (
                abs(expected_year - int(imdb_year)) <= acceptance_year_interval
            )

        runtime_equivalence = True
        if (
            imdb_runtime != "\\N"
            and expected_runtime is not None
            and imdb_runtime is not None
        ):
            runtime_equivalence = (
                abs(expected_runtime - int(imdb_runtime)) <= acceptance_runtime_interval
            )

        return runtime_equivalence and year_equivalence

    def tokenize_IMDB_title(title: str) -> set[str] | None:
        if not title:
            return None
        title = title.lower()
        # Remove startYear (1900–2099)
        title = re.sub(r"\b(19|20)\d{2}\b", "", title)
        # Remove non characters or nums
        re.sub(r"[^\w\s]", " ", title)
        # Collapse multiple spaces
        # title = re.sub(r"\s+", " ", title)
        # Strip leading/trailing spaces
        # title = title.strip()
        # Erase stopwords
        tokens = set(re.findall(r"\w+", title))  # - IMDB_TITLE_STOPWORDS
        return tokens

    def jaccard_title_similarity(a: str, b: str) -> float:
        set_a = MediaTokenUtils.tokenize_IMDB_title(a)
        set_b = MediaTokenUtils.tokenize_IMDB_title(b)

        if not set_a and not set_b:
            return 1.0
        if not set_a or not set_b:
            return 0.0

        return len(set_a & set_b) / len(set_a | set_b)

    def cluster_attribute_jaccard(
        dataframe,
        attribute,
        output_label,
        type_attribute=None,
        default_value=None,
        threshold=0.2,
        og_indexes=["id"],
        blacklist_types=[],
    ):

        def extensive_split(string_value):
            splitted = string_value.lower().split()
            if len(splitted) == 1:
                splitted = splitted[0].split("-")
            return splitted

        def tokenize(string_value):
            return [
                token
                for token in extensive_split(string_value)
                if token not in STOPWORDS_FRANCHISE
            ]

        def jaccard_similarity(set1, set2):
            s1 = set(set1)
            s2 = set(set2)
            return len(s1 & s2) / len(s1 | s2)

        def create_jaccard_similarity_graph(df, token_col, type_col, threshold):
            # Collect all pairs above threshold
            G = nx.Graph()

            if blacklist_types:
                df_search = df[~df[type_col].isin(blacklist_types)]
            else:
                df_search = df
            # instead of titles we use sets of tokens (ex: {"super", "mario", "galaxy"}) and compare each other by combinations of 2
            for (i, t1), (j, t2) in combinations(df_search[token_col].items(), 2):
                # Skip if different media types
                if (
                    type_col
                    and df_search[type_col].iloc[i] != df_search[type_col].iloc[j]
                ):
                    continue
                if jaccard_similarity(t1, t2) >= threshold:
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
            # ["The", Legend", "of", "Zelda"], ["The", "Legend", "of", "Zelda", "II", "The", "Adventure", "of", "Link"]
            split_titles = [extensive_split(df[attribute].iloc[idx]) for idx in cluster]
            # Get largest continuous common words pattern
            label_elements = []
            for words in zip(*split_titles):
                if all(w == words[0] for w in words):
                    label_elements.append(words[0])
                else:
                    break
            return " ".join(label_elements)

        # use positional index for efficiency
        df = dataframe.reset_index(drop=False)
        df[output_label] = default_value
        df["_tokens"] = df[attribute].apply(tokenize)
        # clusters = sets of nodes connected by edges [{node1, node2, ...}, {...}, ...], here nodes are indexes
        clusters_graph = create_jaccard_similarity_graph(
            df, "_tokens", type_attribute, threshold
        )
        clusters = [c for c in nx.connected_components(clusters_graph) if len(c) > 1]

        for cluster in clusters:
            group_label = find_group_label(df, attribute, cluster)
            if group_label:
                # map for each element of cluster their group label
                df.loc[list(cluster), output_label] = group_label

        df.drop(columns=["_tokens"], inplace=True)
        df.set_index(og_indexes, inplace=True)  # restore indexes
        return df

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

    def normalize_title(dataframe, input_col):
        return (
            dataframe[input_col]
            .str.lower()
            .str.replace(r"\b(19|20)\d{2}\b", "", regex=True)
            .str.replace(r"[^\w^\s]", "", regex=True)
            .str.findall(r"\w+")
        )

    def jaccard_title_similarity(a: str, b: str) -> float:
        set_a = set(a) if a else None
        set_b = set(b) if b else None

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
                if type_col and df[type_col].iloc[i] != df[type_col].iloc[j]:
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

        df = df.drop(columns=["_tokens"])
        df = df.set_index(og_indexes)  # restore indexes
        return df

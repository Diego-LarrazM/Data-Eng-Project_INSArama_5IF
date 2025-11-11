from tf_wrangler import TfWrangler

if __name__ == "__main__":
    TransformerWrangler = TfWrangler()

    # Image Testing
    TransformerWrangler.insert_data("test_collection", "test.txt")
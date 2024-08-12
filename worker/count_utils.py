def text_to_words(text):
    cleaned_text = re.sub(r'[^a-zA-Z0-9\s\n]', '', text)
    cleaned_text = cleaned_text.replace("\n", " ")

    words = cleaned_text.lower().split(" ")

    words = list(filter(lambda x : len(x) > 0, words))
    return words

def map_count(text : str): 
    
    inermediate_results = []
    cleaned_chunk = text_to_words(text)
    
    for w in cleaned_chunk:
        inermediate_results.append((w, 1))
        
    return inermediate_results


def reduce_count(intermediate_results : List[Tuple[str, int]]):
    
    counts = dict()
    
    for word, val in intermediate_results:
        if word in counts:
            counts[word] += 1
        else:
            counts[word] = 1
    
    return counts